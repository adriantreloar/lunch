import pytest

from src.lunch.model.fact import (
    Fact,
    FactMeasureMetadatum,
    FactStorage,
    _FactDimensionsMetadata,
    _FactMeasuresMetadata,
)
from src.lunch.model.star_schema import StarSchema
from src.lunch.mvcc.version import Version
from src.lunch.queries.cube_query import CubeQuery
from src.lunch.queries.fully_specified_fact_query import FullySpecifiedFactQuery
from src.lunch.query_engines.cube_query_resolver import CubeQueryResolver

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_VERSION = Version(
    version=1,
    model_version=1,
    reference_data_version=1,
    cube_data_version=1,
    operations_version=1,
    website_version=1,
)

_MEASURE_1 = FactMeasureMetadatum(name="revenue", measure_id=10, type="decimal")
_MEASURE_2 = FactMeasureMetadatum(name="units", measure_id=20, type="integer")

_FACT_WITH_TWO_MEASURES = Fact(
    name="Sales",
    fact_id=1,
    model_version=1,
    dimensions=_FactDimensionsMetadata([]),
    measures=_FactMeasuresMetadata([_MEASURE_1, _MEASURE_2]),
    storage=FactStorage(index_columns=[1], data_columns=[10, 20]),
)

_DIM_REGION = {"name": "Region", "attributes": []}
_DIM_PRODUCT = {"name": "Product", "attributes": []}

_STAR_SCHEMA_TWO_DIMS = StarSchema(
    fact=_FACT_WITH_TWO_MEASURES,
    dimensions={1: _DIM_REGION, 2: _DIM_PRODUCT},
)

_STAR_SCHEMA_NO_DIMS = StarSchema(
    fact=_FACT_WITH_TWO_MEASURES,
    dimensions={},
)


def _cube_query(projection="default", filter=None, aggregation="default") -> CubeQuery:
    return CubeQuery(
        star_schema_name="Sales",
        version="latest",
        projection=projection,
        filter=filter,
        aggregation=aggregation,
    )


# ---------------------------------------------------------------------------
# Tests: default shorthand expansion
# ---------------------------------------------------------------------------


def test_resolve_default_projection_includes_all_dimensions():
    query = _cube_query(projection="default")
    result = CubeQueryResolver.resolve(query, _VERSION, _STAR_SCHEMA_TWO_DIMS)
    dim_ids = [d["dimension_id"] for d in result.dimensions]
    assert 1 in dim_ids
    assert 2 in dim_ids


def test_resolve_default_projection_includes_all_measures():
    query = _cube_query(projection="default")
    result = CubeQueryResolver.resolve(query, _VERSION, _STAR_SCHEMA_TWO_DIMS)
    assert result.measures == [10, 20]


def test_resolve_default_aggregation_maps_to_sum():
    query = _cube_query(aggregation="default")
    result = CubeQueryResolver.resolve(query, _VERSION, _STAR_SCHEMA_NO_DIMS)
    assert result.aggregations == ["sum"]


def test_resolve_none_filter_maps_to_empty_list():
    query = _cube_query(filter=None)
    result = CubeQueryResolver.resolve(query, _VERSION, _STAR_SCHEMA_NO_DIMS)
    assert result.filters == []


def test_resolve_default_projection_zero_dimensions():
    query = _cube_query(projection="default")
    result = CubeQueryResolver.resolve(query, _VERSION, _STAR_SCHEMA_NO_DIMS)
    assert result.dimensions == []


# ---------------------------------------------------------------------------
# Tests: explicit values passed through
# ---------------------------------------------------------------------------


def test_resolve_explicit_projection_passed_through():
    explicit = {"dimensions": [{"dimension_id": 99}], "measures": [42]}
    query = _cube_query(projection=explicit)
    result = CubeQueryResolver.resolve(query, _VERSION, _STAR_SCHEMA_TWO_DIMS)
    assert result.dimensions == [{"dimension_id": 99}]
    assert result.measures == [42]


def test_resolve_explicit_aggregation_passed_through():
    query = _cube_query(aggregation=["mean", "max"])
    result = CubeQueryResolver.resolve(query, _VERSION, _STAR_SCHEMA_NO_DIMS)
    assert result.aggregations == ["mean", "max"]


def test_resolve_explicit_filter_passed_through():
    filters = [{"column": "region", "value": "UK"}]
    query = _cube_query(filter=filters)
    result = CubeQueryResolver.resolve(query, _VERSION, _STAR_SCHEMA_NO_DIMS)
    assert result.filters == filters


def test_resolve_returns_fully_specified_fact_query():
    query = _cube_query()
    result = CubeQueryResolver.resolve(query, _VERSION, _STAR_SCHEMA_NO_DIMS)
    assert isinstance(result, FullySpecifiedFactQuery)


def test_resolve_attaches_version_and_star_schema():
    query = _cube_query()
    result = CubeQueryResolver.resolve(query, _VERSION, _STAR_SCHEMA_TWO_DIMS)
    assert result.version is _VERSION
    assert result.star_schema is _STAR_SCHEMA_TWO_DIMS
