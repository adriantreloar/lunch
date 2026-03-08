from src.lunch.base_classes.data import Data
from src.lunch.model.fact import (
    Fact,
    FactStorage,
    _FactDimensionsMetadata,
    _FactMeasuresMetadata,
)
from src.lunch.model.star_schema import StarSchema
from src.lunch.mvcc.version import Version
from src.lunch.queries.cube_query import CubeQuery
from src.lunch.queries.fully_specified_fact_query import FullySpecifiedFactQuery
from src.lunch.queries.query import Query

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
