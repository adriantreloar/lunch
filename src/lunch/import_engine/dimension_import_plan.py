from typing import NamedTuple


class DimensionImportPlan(NamedTuple):
    """Data collected to allow efficient importing of dimension data"""

    read_dimension: dict
    write_dimension: dict

    read_filter: dict
    merge_key: list

    read_dimension_storage_instructions: dict
    write_dimension_storage_instructions: dict
    data_columns: dict
