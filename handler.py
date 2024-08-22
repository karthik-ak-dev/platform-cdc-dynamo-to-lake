import os
import boto3
import json
import time
from logger_util import Logger
from notification_util import NotificationUtil
from boto3.dynamodb.types import TypeDeserializer
from exceptions import InvalidInputException
from models.user_info import UserInfoModel


ENV = os.getenv("ENV")
athena_boto_cli = boto3.client("athena")
env = os.getenv("ENV")
database_name = os.getenv("CDC_GLUE_DB_NAME")
s3_bucketname = os.getenv("CDC_S3_STORE_BUCKET_NAME")

"""
    Note: Optimal process by processing only the latest known event record
        for the primary key attribute
    Prevents:
        When a same entry has multiple actions, without refining grouping & execution would not
            garauntee the order of the executions - would result in redundancy
        When update actions on same item belongs to the same batch, Hard update methodology would
            result in persisting duplicates, which's avoided
"""


def refine_incoming_records(incoming_records: list):
    def unmarshall_dynamo_dict(dynamo_obj: dict) -> dict:
        deserializer = TypeDeserializer()
        converted_dict = {k: deserializer.deserialize(v) for k, v in dynamo_obj.items()}
        return json.loads(json.dumps(converted_dict, default=str))

    def append_timestamps(data: dict):
        data["record__last_modified_at"] = int(time.time())
        return data

    grpd_uniq_operations = {"INSERT": [], "MODIFY": [], "REMOVE": []}
    uniq_pr_key_operations = {}
    for record in incoming_records:
        event_name = record["eventName"]
        primary_key_cols = record["dynamodb"]["Keys"]
        primary_key_cols: dict = unmarshall_dynamo_dict(primary_key_cols)
        primary_key_cols_identifier = "__".join(primary_key_cols.values())

        new_img = {}
        if event_name == "INSERT" or event_name == "MODIFY":
            new_img: dict = unmarshall_dynamo_dict(record["dynamodb"]["NewImage"])
            new_img = append_timestamps(new_img)

        uniq_pr_key_operations[primary_key_cols_identifier] = {
            "event_name": event_name,
            "primary_key_cols": primary_key_cols,
            "new_img": new_img,
        }

    for each in uniq_pr_key_operations.values():
        grpd_uniq_operations[each["event_name"]].append(each)

    return grpd_uniq_operations


def fetch_model_factory(**kwargs):
    def get_factory_identifier(table_name: str):
        frmt_t_name = table_name
        frmt_t_name = frmt_t_name.replace("_iceberg_table", "")

        return frmt_t_name

    factories = {
        "cdc_dynamo_to_lake_user_info": UserInfoModel(**kwargs),
        # Add more models as per the need
    }

    table_name = kwargs.get("table_name", None)
    factory_identfier = get_factory_identifier(table_name)
    factory = factories.get(factory_identfier, None)
    if not factory:
        raise InvalidInputException(
            msg=f"Invalid factory identifier: {factory_identfier}; table_name: {table_name}"
        )

    return factory


def handler(event, context):
    Logger.set_trace_event_id()

    try:
        Logger.info_log(f"In handler: handler, event - {event}")
        records = event.get("Records")

        Logger.info_log(f"Total records count: {len(records)}")

        dynamo_table_name: str = (
            records[0]["eventSourceARN"].split(":table/")[1].split("/stream")[0]
        )

        q_output_location = f"s3://{s3_bucketname}/dynamo-iceberg-tables/{dynamo_table_name}/logs/athena_output/"

        Model = fetch_model_factory(
            athena_boto_cli=athena_boto_cli,
            database_name=database_name,
            table_name=dynamo_table_name,
            q_output_location=q_output_location,
            catalog_name=None,
        )

        grouped_records: dict = refine_incoming_records(records)
        for event_name, records in grouped_records.items():
            if not records:
                continue

            if event_name == "INSERT":
                response = Model.insert(records)
            elif event_name == "MODIFY":
                response = Model.hard_update(records)
            elif event_name == "REMOVE":
                response = Model.delete(records)
            else:
                pass

            Logger.error_log(f"In handler: handler, response - {str(response)}")
        return "Successfully Processed"
    except Exception as err:
        Logger.error_log(f"Error in CDC syncing: {str(err)}")
        NotificationUtil().notify_on_slack(
            {
                "text": f"{ENV.upper()} - Error in CDC Ingestion, Table: {dynamo_table_name}",
                "attachments": [{"text": f"Error: {str(err)}"}],
            }
        )
        raise err
