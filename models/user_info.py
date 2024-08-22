from models.abstract import ModelBaseAbstract


class UserInfoModel(ModelBaseAbstract):
    def __init__(
        self,
        athena_boto_cli,
        database_name,
        table_name,
        q_output_location,
        catalog_name=None,
    ) -> None:
        super().__init__(
            athena_boto_cli, database_name, table_name, q_output_location, catalog_name
        )

    def schema(self):
        return {
            # Added at the iceberg table level
            "record__last_modified_at": "string",
            # Actual data-source
            "user_id": "string",
            "first_name": "string",
            "last_name": "string",
        }
