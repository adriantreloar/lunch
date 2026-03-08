from uuid import uuid4

from src.lunch.base_classes.data import Data
from src.lunch.model.fact import (
    Fact,
    FactStorage,
    _FactDimensionsMetadata,
    _FactMeasuresMetadata,
)
from src.lunch.model.star_schema import StarSchema
from src.lunch.mvcc.version import Version
from src.lunch.queries.basic_query import BasicQuery
from src.lunch.queries.cube_query import CubeQuery
from src.lunch.queries.fully_specified_fact_query import FullySpecifiedFactQuery
from src.lunch.queries.query import Query
from src.lunch.queries.resolved_cube_query import ResolvedCubeQuery
from src.lunch.queries.serial_query import SerialQuery

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_FACT = Fact(
    name="Sales",
    fact_id=1,
    model_version=1,
    dimensions=_FactDimensionsMetadata([]),
    measures=_FactMeasuresMetadata([]),
    storage=FactStorage(index_columns=[1], data_columns=[]),
)
_STAR_SCHEMA = StarSchema(fact=_FACT, dimensions={})
_VERSION = Version(
    version=1,
    model_version=1,
    reference_data_version=1,
    cube_data_version=1,
    operations_version=1,
    website_version=1,
)


# ---------------------------------------------------------------------------
# Query (abstract base)
# ---------------------------------------------------------------------------


def test_query_is_data_subclass():
    assert issubclass(Query, Data)


# ---------------------------------------------------------------------------
# CubeQuery
# ---------------------------------------------------------------------------


def test_cube_query_is_data_instance():
    q = CubeQuery(
        star_schema_name="Sales",
        version="latest",
        projection="default",
        filter=None,
        aggregation="default",
    )
    assert isinstance(q, Data)
    assert isinstance(q, Query)


def test_cube_query_stores_fields():
    q = CubeQuery(
        star_schema_name="Sales",
        version="latest",
        projection="default",
        filter=None,
        aggregation="default",
    )
    assert q.star_schema_name == "Sales"
    assert q.version == "latest"
    assert q.projection == "default"
    assert q.filter is None
    assert q.aggregation == "default"


def test_cube_query_accepts_shorthand_version_latest():
    q = CubeQuery(
        star_schema_name="X",
        version="latest",
        projection=None,
        filter=None,
        aggregation=None,
    )
    assert q.version == "latest"


def test_cube_query_accepts_shorthand_projection_default():
    q = CubeQuery(
        star_schema_name="X",
        version=1,
        projection="default",
        filter=None,
        aggregation=None,
    )
    assert q.projection == "default"


def test_cube_query_accepts_none_filter():
    q = CubeQuery(
        star_schema_name="X",
        version="latest",
        projection="default",
        filter=None,
        aggregation="default",
    )
    assert q.filter is None


# ---------------------------------------------------------------------------
# FullySpecifiedFactQuery
# ---------------------------------------------------------------------------


def test_fully_specified_fact_query_is_data_instance():
    q = FullySpecifiedFactQuery(
        star_schema=_STAR_SCHEMA,
        version=_VERSION,
        dimensions=[],
        measures=[],
        filters=[],
        aggregations=[],
    )
    assert isinstance(q, Data)
    assert isinstance(q, Query)


def test_fully_specified_fact_query_stores_fields():
    dim = {"id_": 1, "name": "Time"}
    q = FullySpecifiedFactQuery(
        star_schema=_STAR_SCHEMA,
        version=_VERSION,
        dimensions=[dim],
        measures=[10, 20],
        filters=["filter_a"],
        aggregations=["sum"],
    )
    assert q.star_schema is _STAR_SCHEMA
    assert q.version is _VERSION
    assert q.dimensions == [dim]
    assert q.measures == [10, 20]
    assert q.filters == ["filter_a"]
    assert q.aggregations == ["sum"]


def test_fully_specified_fact_query_stores_concrete_version():
    q = FullySpecifiedFactQuery(
        star_schema=_STAR_SCHEMA,
        version=_VERSION,
        dimensions=[],
        measures=[],
        filters=[],
        aggregations=[],
    )
    assert isinstance(q.version, Version)


def test_fully_specified_fact_query_stores_star_schema_object():
    q = FullySpecifiedFactQuery(
        star_schema=_STAR_SCHEMA,
        version=_VERSION,
        dimensions=[],
        measures=[],
        filters=[],
        aggregations=[],
    )
    assert isinstance(q.star_schema, StarSchema)


# ---------------------------------------------------------------------------
# ResolvedCubeQuery
# ---------------------------------------------------------------------------


def test_resolved_cube_query_is_data_instance():
    q = ResolvedCubeQuery(
        star_schema=_STAR_SCHEMA,
        version=_VERSION,
        projection={"dimensions": [], "measures": []},
        aggregation=["sum"],
    )
    assert isinstance(q, Data)
    assert isinstance(q, Query)


def test_resolved_cube_query_stores_fields():
    projection = {"dimensions": [1, 2], "measures": [10]}
    aggregation = ["sum"]
    q = ResolvedCubeQuery(
        star_schema=_STAR_SCHEMA,
        version=_VERSION,
        projection=projection,
        aggregation=aggregation,
    )
    assert q.star_schema is _STAR_SCHEMA
    assert q.version is _VERSION
    assert q.projection is projection
    assert q.aggregation is aggregation


def test_resolved_cube_query_holds_concrete_version():
    q = ResolvedCubeQuery(
        star_schema=_STAR_SCHEMA,
        version=_VERSION,
        projection="default",
        aggregation="sum",
    )
    assert isinstance(q.version, Version)


# ---------------------------------------------------------------------------
# BasicQuery
# ---------------------------------------------------------------------------


def test_basic_query_is_data_instance():
    q = BasicQuery(name="FetchData", inputs={}, outputs={})
    assert isinstance(q, Data)
    assert isinstance(q, Query)


def test_basic_query_stores_fields():
    out_uuid = uuid4()
    inputs = {"dimension_id": 1}
    outputs = {out_uuid: out_uuid}
    q = BasicQuery(name="FetchDimensionData", inputs=inputs, outputs=outputs)
    assert q.name == "FetchDimensionData"
    assert q.inputs is inputs
    assert q.outputs is outputs


def test_basic_query_accepts_empty_inputs_and_outputs():
    q = BasicQuery(name="Noop", inputs={}, outputs={})
    assert q.inputs == {}
    assert q.outputs == {}


# ---------------------------------------------------------------------------
# SerialQuery
# ---------------------------------------------------------------------------


def test_serial_query_is_data_instance():
    q = SerialQuery(steps=[], outputs=set())
    assert isinstance(q, Data)
    assert isinstance(q, Query)


def test_serial_query_stores_steps_and_outputs():
    step_a = BasicQuery(name="StepA", inputs={}, outputs={})
    step_b = BasicQuery(name="StepB", inputs={}, outputs={})
    out_uuid = uuid4()
    q = SerialQuery(steps=[step_a, step_b], outputs={out_uuid})
    assert q.steps == [step_a, step_b]
    assert q.outputs == {out_uuid}


def test_serial_query_preserves_step_order():
    steps = [BasicQuery(name=f"Step{i}", inputs={}, outputs={}) for i in range(3)]
    q = SerialQuery(steps=steps, outputs=set())
    assert q.steps[0].name == "Step0"
    assert q.steps[1].name == "Step1"
    assert q.steps[2].name == "Step2"


def test_serial_query_empty():
    q = SerialQuery(steps=[], outputs=set())
    assert q.steps == []
    assert q.outputs == set()
