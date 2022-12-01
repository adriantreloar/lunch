from typing import NamedTuple


class FactAppendPlan(NamedTuple):
    """Data collected to allow efficient importing of fact data"""

    read_fact: dict
    write_fact: dict

    read_filter: dict
    merge_key: list

    read_fact_storage_instructions: dict
    write_fact_storage_instructions: dict
    data_columns: dict
