import os
from pathlib import Path
from dagster import (
    Definitions, 
    MultiPartitionsDefinition, 
    StaticPartitionsDefinition, 
)
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator

# 1. Define the partitioning scheme
date_partitions = MultiPartitionsDefinition({
    "year": StaticPartitionsDefinition(["2024", "2025", "2026"]),
    "month": StaticPartitionsDefinition([str(i).zfill(2) for i in range(1, 13)])
})

# Point to the dbt project directory
DBT_PROJECT_DIR = Path(__file__).joinpath("..", "..", "..", "nyc_taxi_motherduck").resolve()

class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props):
        # This automatically groups the assets by their dbt schema/folder
        return dbt_resource_props.get("fqn")[1] 

@dbt_assets(
    manifest=DBT_PROJECT_DIR.joinpath("target", "manifest.json"),
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
    partitions_def=date_partitions,
    op_tags={"dagster/max_coordinate_concurrency": 4}
)
def nyc_taxi_assets(context, dbt: DbtCliResource):
    # Extract the partition keys for the current run
    partition_key = context.partition_key.keys_by_dimension
    year = partition_key["year"]
    month = partition_key["month"]

    # Pass the keys to dbt as variables
    dbt_vars = {"year": year, "month": month}
    
    yield from dbt.cli(["build", "--vars", str(dbt_vars)], context=context).stream()

defs = Definitions(
    assets=[nyc_taxi_assets],
    resources={
        "dbt": DbtCliResource(project_dir=str(DBT_PROJECT_DIR)),
    },
)
