"""Tests for CubeQueryContextResolver."""

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
from src.lunch.queries.resolved_cube_query import ResolvedCubeQuery
from src.lunch.query_engines.cube_query_context_resolver import CubeQueryContextResolver

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

_FACT_WITH_MEASURES = Fact(
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
    fact=_FACT_WITH_MEASURES,
    dimensions={1: _DIM_REGION, 2: _DIM_PRODUCT},
)

_STAR_SCHEMA_NO_DIMS = StarSchema(
    fact=_FACT_WITH_MEASURES,
    dimensions={},
)


def _cube_query(projection="default", aggregation="default", filter=None) -> CubeQuery:
    return CubeQuery(
        star_schema_name="Sales",
        version="latest",
        projection=projection,
        filter=filter,
        aggregation=aggregation,
    )


# ---------------------------------------------------------------------------
# Return type
# ---------------------------------------------------------------------------


def test_resolve_returns_resolved_cube_query():
    query = _cube_query()
    result = CubeQueryContextResolver.resolve(query, _VERSION, _STAR_SCHEMA_NO_DIMS)
    assert isinstance(result, ResolvedCubeQuery)


def test_resolve_attaches_version_and_star_schema():
    query = _cube_query()
    result = CubeQueryContextResolver.resolve(query, _VERSION, _STAR_SCHEMA_TWO_DIMS)
    assert result.version is _VERSION
    assert result.star_schema is _STAR_SCHEMA_TWO_DIMS


# ---------------------------------------------------------------------------
# Default projection resolution
# ---------------------------------------------------------------------------


def test_resolve_default_projection_includes_all_dimension_ids():
    query = _cube_query(projection="default")
    result = CubeQueryContextResolver.resolve(query, _VERSION, _STAR_SCHEMA_TWO_DIMS)
    dim_ids = [d["dimension_id"] for d in result.projection["dimensions"]]
    assert 1 in dim_ids
    assert 2 in dim_ids


def test_resolve_default_projection_includes_all_measures():
    query = _cube_query(projection="default")
    result = CubeQueryContextResolver.resolve(query, _VERSION, _STAR_SCHEMA_TWO_DIMS)
    assert result.projection["measures"] == [10, 20]


def test_resolve_default_projection_zero_dimensions_gives_empty_list():
    query = _cube_query(projection="default")
    result = CubeQueryContextResolver.resolve(query, _VERSION, _STAR_SCHEMA_NO_DIMS)
    assert result.projection["dimensions"] == []


def test_resolve_default_projection_returns_dict_with_dimensions_and_measures_keys():
    query = _cube_query(projection="default")
    result = CubeQueryContextResolver.resolve(query, _VERSION, _STAR_SCHEMA_NO_DIMS)
    assert "dimensions" in result.projection
    assert "measures" in result.projection


# ---------------------------------------------------------------------------
# Explicit projection passed through
# ---------------------------------------------------------------------------


def test_resolve_explicit_projection_passed_through():
    explicit = {"dimensions": [{"dimension_id": 99}], "measures": [42]}
    query = _cube_query(projection=explicit)
    result = CubeQueryContextResolver.resolve(query, _VERSION, _STAR_SCHEMA_TWO_DIMS)
    assert result.projection is explicit


# ---------------------------------------------------------------------------
# Aggregation resolution
# ---------------------------------------------------------------------------


def test_resolve_default_aggregation_maps_to_sum_list():
    query = _cube_query(aggregation="default")
    result = CubeQueryContextResolver.resolve(query, _VERSION, _STAR_SCHEMA_NO_DIMS)
    assert result.aggregation == ["sum"]


def test_resolve_explicit_aggregation_passed_through_as_list():
    query = _cube_query(aggregation=["mean", "max"])
    result = CubeQueryContextResolver.resolve(query, _VERSION, _STAR_SCHEMA_NO_DIMS)
    assert result.aggregation == ["mean", "max"]
