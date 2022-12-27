from src.lunch.base_classes.transformer import Transformer
from src.lunch.plans.plan import Plan
from src.lunch.plans.basic_plan import BasicPlan
from src.lunch.plans.remote_plan import RemotePlan
from src.lunch.model.star_schema import StarSchema, StarSchemaTransformer
from src.lunch.model.table_metadata import TableMetadata, TableMetadataTransformer
from src.lunch.mvcc.version import Version

class FactAppendPlanner(Transformer):
    def __init__(self):
        pass

    @staticmethod
    def create_local_dataframe_append_plan(
        read_version_target_model: StarSchema,
        write_version_target_model: StarSchema,
        source_metadata: TableMetadata,
        column_mapping: dict,
        read_version: Version,
        write_version: Version,
    ) -> Plan:
        print(write_version_target_model)
        print(source_metadata)
        for m in column_mapping:
            print(m)

        # Check every dimension is mapped (in theory there could be a default - 0? meaning ALL?)
        # Check every mapping maps a source to a target
        # for each mapping work out what the target type is - id, translate, measure_name, or measure

        # Translate, and Broadcast Measures in parallel

        #Combine - all? - in storage order index followed by data followed by values
        # Sort by key
        #

        return BasicPlan(
            name="",
            inputs={
                    },
            outputs={}
        )
