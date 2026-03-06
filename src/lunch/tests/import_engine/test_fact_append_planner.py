import numpy as np
import pytest

from src.lunch.import_engine.fact_append_planner import FactAppendPlanner
from src.lunch.model.fact import (
    Fact,
    FactDimensionMetadatum,
    FactMeasureMetadatum,
    FactStorage,
    _FactDimensionsMetadata,
    _FactMeasuresMetadata,
)
from src.lunch.model.star_schema import StarSchema
from src.lunch.model.table_metadata import TableMetadata
from src.lunch.mvcc.version import Version
from src.lunch.plans.basic_plan import BasicPlan

v0 = Version(
    version=0, model_version=0, reference_data_version=0, cube_data_version=0, operations_version=0, website_version=0
)
v1 = Version(
    version=1, model_version=1, reference_data_version=1, cube_data_version=0, operations_version=0, website_version=0
)

_DIMS = _FactDimensionsMetadata(
    [
        FactDimensionMetadatum(
            name="Department", column_id=3, view_order=1, dimension_name="Department", dimension_id=1
        ),
        FactDimensionMetadatum(name="Time", column_id=2, view_order=2, dimension_name="Time", dimension_id=2),
    ]
)
_MEASURES = _FactMeasuresMetadata(
    [
        FactMeasureMetadatum(name="sales", measure_id=1, type="decimal", precision=2),
    ]
)
_FACT = Fact(
    name="Sales",
    fact_id=1,
    model_version=1,
    dimensions=_DIMS,
    measures=_MEASURES,
    storage=FactStorage(index_columns=[1], data_columns=[2, 3]),
)
_SCHEMA = StarSchema(fact=_FACT, dimensions={})

_COLUMN_MAPPING = [
    {"source": ["dept_col"], "target": ["Department", "id_"]},
    {"source": ["time_col"], "target": ["Time", "thing"]},
    {"source": ["sales_col"], "measure target": ["measures", "sales"]},
]

_SOURCE_META = TableMetadata(
    column_names=["dept_col", "time_col", "sales_col"],
    column_types=[np.dtype("int64"), np.dtype("int64"), np.dtype("float64")],
    length=5,
)


def _make_plan():
    return FactAppendPlanner.create_local_dataframe_append_plan(
        read_version_target_model=_SCHEMA,
        write_version_target_model=_SCHEMA,
        source_metadata=_SOURCE_META,
        column_mapping=_COLUMN_MAPPING,
        read_version=v0,
        write_version=v1,
    )


# ---------------------------------------------------------------------------
# plan name and type
# ---------------------------------------------------------------------------


def test_plan_is_basic_plan_with_correct_name():
    plan = _make_plan()
    assert isinstance(plan, BasicPlan)
    assert plan.name == "_import_fact_append_locally_from_dataframe"


# ---------------------------------------------------------------------------
# column_id_mapping: dimension columns
# ---------------------------------------------------------------------------


def test_dimension_source_column_maps_to_column_id():
    plan = _make_plan()
    assert plan.inputs["column_id_mapping"]["dept_col"] == 3  # Department.column_id
    assert plan.inputs["column_id_mapping"]["time_col"] == 2  # Time.column_id


# ---------------------------------------------------------------------------
# column_id_mapping: measure columns
# ---------------------------------------------------------------------------


def test_measure_source_column_maps_to_measure_id():
    plan = _make_plan()
    assert plan.inputs["column_id_mapping"]["sales_col"] == 1  # sales.measure_id


# ---------------------------------------------------------------------------
# other plan inputs / outputs
# ---------------------------------------------------------------------------


def test_merge_key_matches_storage_index_columns():
    plan = _make_plan()
    assert plan.inputs["merge_key"] == [1]


def test_read_fact_is_taken_from_read_schema():
    plan = _make_plan()
    assert plan.inputs["read_fact"] is _SCHEMA.fact


def test_write_fact_is_taken_from_write_schema():
    plan = _make_plan()
    assert plan.outputs["write_fact"] is _SCHEMA.fact


def test_read_filter_is_none():
    plan = _make_plan()
    assert plan.inputs["read_filter"] is None


def test_source_definition_has_correct_length():
    plan = _make_plan()
    assert plan.inputs["source_definition"]["length"] == 5


def test_source_definition_contains_all_source_column_names():
    plan = _make_plan()
    cols = plan.inputs["source_definition"]["columns"]
    assert set(cols.keys()) == {"dept_col", "time_col", "sales_col"}


# ---------------------------------------------------------------------------
# error paths: unresolvable dimension / measure name
# ---------------------------------------------------------------------------


def test_unknown_dimension_name_raises_stop_iteration():
    bad_mapping = [{"source": ["x"], "target": ["NoSuchDimension", "id_"]}]
    with pytest.raises(StopIteration):
        FactAppendPlanner.create_local_dataframe_append_plan(
            read_version_target_model=_SCHEMA,
            write_version_target_model=_SCHEMA,
            source_metadata=_SOURCE_META,
            column_mapping=bad_mapping,
            read_version=v0,
            write_version=v1,
        )


def test_unknown_measure_name_raises_stop_iteration():
    bad_mapping = [{"source": ["x"], "measure target": ["measures", "no_such_measure"]}]
    with pytest.raises(StopIteration):
        FactAppendPlanner.create_local_dataframe_append_plan(
            read_version_target_model=_SCHEMA,
            write_version_target_model=_SCHEMA,
            source_metadata=_SOURCE_META,
            column_mapping=bad_mapping,
            read_version=v0,
            write_version=v1,
        )
