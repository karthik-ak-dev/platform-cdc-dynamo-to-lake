import time
from logger_util import Logger


class AthenaUtil:
    def __init__(self, client, database_name, q_output_location) -> None:
        self.client = client
        self.database_name = database_name
        self.q_output_location = q_output_location
        self.wait_time = 4
        self.retry_wait_time = 3

    def run_query(self, query):
        response = self.client.start_query_execution(
            QueryExecutionContext={"Database": self.database_name},
            QueryString=query,
            ResultConfiguration={"OutputLocation": self.q_output_location},
        )
        time.sleep(self.wait_time)
        query_status = self.client.get_query_execution(
            QueryExecutionId=response["QueryExecutionId"]
        )
        query_execution_status = query_status["QueryExecution"]["Status"]["State"]
        while query_execution_status in ("RUNNING", "QUEUED"):
            time.sleep(self.retry_wait_time)
            query_status = self.client.get_query_execution(
                QueryExecutionId=response["QueryExecutionId"]
            )
            query_execution_status = query_status["QueryExecution"]["Status"]["State"]
            Logger.info_log(
                f"executing statement : {query } ==> QueryExecutionId : {response['QueryExecutionId']}, status : {query_execution_status}"
            )
        if query_execution_status != "SUCCEEDED":
            raise Exception(
                f"executing statement : {query } ==> QueryExecutionId : {response['QueryExecutionId']}, Error : {query_status['QueryExecution']['Status']}"
            )

        response = self.client.get_query_execution(
            QueryExecutionId=response["QueryExecutionId"]
        )
        Logger.info_log(f"Query execution status : {response}")

        return response
