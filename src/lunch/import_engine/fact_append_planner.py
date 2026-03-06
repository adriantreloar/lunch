from src.lunch.base_classes.transformer import Transformer
from src.lunch.model.star_schema import StarSchema, StarSchemaTransformer
from src.lunch.model.table_metadata import TableMetadata, TableMetadataTransformer
from src.lunch.mvcc.version import Version
from src.lunch.plans.basic_plan import BasicPlan
from src.lunch.plans.plan import Plan
from src.lunch.plans.remote_plan import RemotePlan


class FactAppendPlanner(Transformer):
    @staticmethod
    def create_local_dataframe_append_plan(
        read_version_target_model: StarSchema,
        write_version_target_model: StarSchema,
        source_metadata: TableMetadata,
        column_mapping: dict,
        read_version: Version,
        write_version: Version,
    ) -> Plan:
        write_fact = write_version_target_model.fact

        source_column_definition = {
            name: str(dtype_) for name, dtype_ in zip(source_metadata.column_names, source_metadata.column_types)
        }
        source_length = source_metadata.length

        column_id_mapping: dict[str, int] = {}
        for mapping in column_mapping:
            source_col = mapping["source"][0]
            if "target" in mapping:
                dim_name = mapping["target"][0]
                dim_meta = next(d for d in write_fact.dimensions if d.dimension_name == dim_name)
                column_id_mapping[source_col] = dim_meta.column_id
            elif "measure target" in mapping:
                measure_name = mapping["measure target"][1]
                measure_meta = next(m for m in write_fact.measures if m.name == measure_name)
                column_id_mapping[source_col] = measure_meta.measure_id

        return BasicPlan(
            name="_import_fact_append_locally_from_dataframe",
            inputs={
                "source_definition": {
                    "type": "pd.DataFrame",
                    "length": source_length,
                    "columns": source_column_definition,
                },
                "read_fact": read_version_target_model.fact,
                "column_id_mapping": column_id_mapping,
                "merge_key": list(write_version_target_model.fact.storage.index_columns),
                "read_filter": None,
            },
            outputs={
                "write_fact": write_version_target_model.fact,
            },
        )
