import os
from pathlib import Path
from dagster import Definitions
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator

# Point to your dbt project directory
DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "..", "nyc_taxi_motherduck").resolve()

class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props):
        # This automatically groups your assets by their dbt schema/folder
        return dbt_resource_props.get("fqn")[1] 

@dbt_assets(
    manifest=DBT_PROJECT_DIR.joinpath("target", "manifest.json"),
    dagster_dbt_translator=CustomDagsterDbtTranslator()
)
def nyc_taxi_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

defs = Definitions(
    assets=[nyc_taxi_assets],
    resources={
        "dbt": DbtCliResource(project_dir=str(DBT_PROJECT_DIR)),
    },
)
