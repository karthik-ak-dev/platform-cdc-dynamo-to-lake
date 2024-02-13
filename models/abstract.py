import json
from abc import ABC, abstractmethod
from athena_util import AthenaUtil
from logger_util import Logger
from meta_syncer import IcebergTableMetaSyncer


class ModelBaseAbstract(ABC):
    def __init__(
        self,
        athena_boto_cli,
        database_name,
        table_name,
        q_output_location,
        catalog_name,
    ):
        self.table_name = table_name
        self.iceberg_meta_syncer = IcebergTableMetaSyncer(
            athena_boto_cli, database_name, table_name, q_output_location, catalog_name
        )
        self.athena_util = AthenaUtil(athena_boto_cli, database_name, q_output_location)

    @abstractmethod
    def schema(self):
        pass

    def insert(self, insert_records):
        self.iceberg_meta_syncer.clean_expired_data()

        # self.sync_meta() -> Deprecated instead using append_cols_if_not_exists
        self.iceberg_meta_syncer.append_cols_if_not_exists(self.schema())

        table_cols = (self.schema()).keys()
        col_nm_list_str = ", ".join(table_cols)

        multiple_col_val_list = []
        for record in insert_records:
            Tblvalues = record["new_img"]

            col_val_list = []
            for meta_col_key in table_cols:
                data_value = Tblvalues.get(meta_col_key, "")
                data_value: str = str(data_value).replace("'", "")
                stringyfied_data_val = json.dumps(data_value).replace('"', "")
                col_val_list.append(f"'{str(stringyfied_data_val)}'")

            col_val_list_str = "(" + ", ".join(col_val_list) + ")"
            multiple_col_val_list.append(col_val_list_str)

        insert_stmt = f"insert into {self.table_name} ({col_nm_list_str}) values {', '.join(multiple_col_val_list)}"
        Logger.info_log(f"Insert operation on {self.table_name}, query - {insert_stmt}")

        return self.athena_util.run_query(insert_stmt)

    def delete(self, delete_records):
        multiple_del_where_col_list = []

        for record in delete_records:
            primary_key_cols = record["primary_key_cols"]

            del_where_col_list = []
            for k, v in primary_key_cols.items():
                del_where_col_list.append(f" {k} = '{v}' ")

            del_where_col_str = " and ".join(del_where_col_list)
            multiple_del_where_col_list.append(f"({del_where_col_str})")

        multiple_del_where_col_str = (
            f" where {' or '.join(multiple_del_where_col_list)}"
        )
        del_stmt = f"delete from {self.table_name} {multiple_del_where_col_str}"
        Logger.info_log(f"Delete operation on {self.table_name}, query - {del_stmt}")

        return self.athena_util.run_query(del_stmt)

    def hard_update(self, update_records):
        Logger.info_log(f"In hard update record count: {len(update_records)}")

        resp = self.delete(update_records)
        Logger.info_log(f"In hard update post deleting records: {str(resp)}")

        resp = self.insert(update_records)
        Logger.info_log(f"In hard update post inserting records: {str(resp)}")

        return

    # Deprecated below abstract methods as they are moved as above
    # @abstractmethod
    # def insert(self):
    #     pass

    # @abstractmethod
    # def update(self):
    #     pass

    # @abstractmethod
    # def delete(self):
    #     pass

    # @abstractmethod
    # def hard_update(self):
    #     pass

    # Meta col map syncer - Deprecated (Using append_cols_if_not_exists logic)
    # def sync_meta(self) -> None:
    #     return self.iceberg_meta_syncer.sync_meta_cols((self.schema()))

    # def meta_col_map(self):
    #     meta_cols = self.iceberg_meta_syncer.sync_and_fetch_meta_cols(self.schema())
    #     return { item["Name"]: item["Type"] for item in meta_cols }
