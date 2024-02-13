from models.abstract import ModelBaseAbstract


class AchievementsInfoModel(ModelBaseAbstract):
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
            "achievement_id": "string",
            "achievement_criteria": "string",
            "achievement_level": "string",
            "achievement_name": "string",
            "achievement_type": "string",
            "target_count": "string",
            "web3_reference_id": "string",
        }
