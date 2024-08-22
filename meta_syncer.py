from athena_util import AthenaUtil
from logger_util import Logger


class IcebergTableMetaSyncer:
    def __init__(
        self, client, database_name, table_name, q_output_location, catalog_name=None
    ) -> None:
        self.client = client
        self.database_name: str = database_name
        self.table_name: str = table_name
        self.q_output_location: str = q_output_location
        self.catalog_name: str = catalog_name or "AWSDATACATALOG"
        self.athen_util = AthenaUtil(
            self.client, self.database_name, self.q_output_location
        )

    def clean_expired_data(self):
        vaccum_stmt = f"vacuum {self.table_name}"
        Logger.info_log(f"Vacumm operation on {self.table_name}, query - {vaccum_stmt}")
        return self.athen_util.run_query(vaccum_stmt)

    def get_current_table_cols(self):
        show_cols_stmt = f"show columns from {self.table_name}"
        Logger.info_log(
            f"Show columns operation on {self.table_name}, query - {show_cols_stmt}"
        )
        return self.athen_util.run_query(show_cols_stmt)

    def append_cols_if_not_exists(self, expected_curr_schema: dict):
        try:
            curr_cols_q_resp = self.get_current_table_cols()
            query_results = self.client.get_query_results(
                QueryExecutionId=curr_cols_q_resp["QueryExecution"]["QueryExecutionId"]
            )
            Logger.info_log(
                f"In append_cols_if_not_exists for table: {self.table_name}, show cols query_results: {str(query_results)}"
            )

            result_set = query_results["ResultSet"]
            if "Rows" in result_set and len(result_set["Rows"]) > 0:
                existing_column_names = [
                    data["Data"][0]["VarCharValue"] for data in result_set["Rows"]
                ]
                Logger.info_log(
                    f"In append_cols_if_not_exists for table: {self.table_name}, existing_column_names: {str(existing_column_names)}"
                )

                expected_column_names = list(expected_curr_schema.keys())
                Logger.info_log(
                    f"In append_cols_if_not_exists for table: {self.table_name}, expected_column_names: {str(expected_column_names)}"
                )

                new_cols = list(set(expected_column_names) - set(existing_column_names))
                Logger.info_log(
                    f"In append_cols_if_not_exists for table: {self.table_name}, new_cols: {str(new_cols)}"
                )

                if new_cols:
                    new_attributes = {
                        f"{new_col}": expected_curr_schema[new_col]
                        for new_col in new_cols
                    }
                    self.update_meta(new_attributes)
            else:
                raise Exception(f"No columns found for the table: {self.table_name}")

            return
        except Exception as e:
            Logger.error_log(
                f"Error in append_cols_if_not_exists for table: {self.table_name} - {str(e)}"
            )
            raise (e)

    def update_meta(self, new_attributes):
        alter_table_sql_stmt = f"ALTER TABLE {self.table_name} ADD COLUMNS ("

        columns = []
        for attribute_name, attribute_type in new_attributes.items():
            columns.append(f"{attribute_name} {attribute_type}")

        alter_table_sql_stmt += ", ".join(columns)
        alter_table_sql_stmt += ")"

        response = self.athen_util.run_query(alter_table_sql_stmt)

        return response

    # Meta col map syncer - Deprecated (Using append_cols_if_not_exists logic)
    # def retrieve_meta(self):
    #     try:
    #         athenaTblMd = self.client.get_table_metadata(
    #             CatalogName=self.catalog_name,
    #             DatabaseName=self.database_name,
    #             TableName=self.table_name
    #         )
    #     except Exception as e:
    #         Logger.error_log(f"Athena Table Metadata retrieval function Failed.Please check exception - {str(e)}")
    #         raise(e)
    #     else:
    #         return athenaTblMd

    # def retrieve_meta_cols(self):
    #     athenaTblMd = self.retrieve_meta()
    #     try:
    #         AthenTblMdCols = athenaTblMd['TableMetadata']['Columns']
    #     except Exception as e:
    #         Logger.error_log(f"Athena Metadata does not have column information. Please check table {self.table_name} and database {self.database_name}")
    #         raise(e)
    #     else:
    #         return AthenTblMdCols

    # def is_meta_cols_in_sync(self, curr_meta_cols, expected_curr_schema):
    #     existing_cols = [column['Name'] for column in curr_meta_cols]
    #     expected_cols = list(expected_curr_schema.keys())
    #     new_cols = list(set(expected_cols) - set(existing_cols))

    #     if(len(new_cols)) == 0: return True, {}

    #     new_attributes = {f"{new_col}": expected_curr_schema[new_col] for new_col in new_cols}

    #     return False, new_attributes

    # def sync_meta_cols(self, expected_curr_schema) -> None:
    #     self.clean_expired_data()
    #     curr_meta_cols = self.retrieve_meta_cols()
    #     is_meta_in_sync_state, new_attributes = self.is_meta_cols_in_sync(curr_meta_cols, expected_curr_schema)
    #     if is_meta_in_sync_state: return

    #     self.update_meta(new_attributes)

    #     return

    # def sync_and_fetch_meta_cols(self, expected_curr_schema):
    #     self.clean_expired_data()
    #     curr_meta_cols = self.retrieve_meta_cols()
    #     is_meta_in_sync_state, new_attributes = self.is_meta_cols_in_sync(curr_meta_cols, expected_curr_schema)
    #     if is_meta_in_sync_state:
    #         return curr_meta_cols

    #     self.update_meta(new_attributes)

    #     return self.retrieve_meta_cols()
