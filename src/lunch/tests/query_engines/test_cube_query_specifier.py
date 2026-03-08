import pytest
from mock import AsyncMock, Mock, call

from src.lunch.model.fact import Fact, FactStorage, _FactDimensionsMetadata, _FactMeasuresMetadata
from src.lunch.model.star_schema import StarSchema
from src.lunch.mvcc.version import Version
from src.lunch.queries.cube_query import CubeQuery
from src.lunch.queries.fully_specified_fact_query import FullySpecifiedFactQuery
from src.lunch.query_engines.cube_query_resolver import CubeQueryResolver
from src.lunch.query_engines.cube_query_specifier import CubeQuerySpecifier

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_VERSION = Version(
    version=5,
    model_version=5,
    reference_data_version=5,
    cube_data_version=5,
    operations_version=5,
    website_version=5,
)

_FACT = Fact(
    name="Sales",
    fact_id=1,
    model_version=1,
    dimensions=_FactDimensionsMetadata([]),
    measures=_FactMeasuresMetadata([]),
    storage=FactStorage(index_columns=[1], data_columns=[]),
)
_STAR_SCHEMA = StarSchema(fact=_FACT, dimensions={})


def _make_specifier(version=None, star_schema=None):
    version_manager = AsyncMock()
    version_manager.read_version.return_value = version or _VERSION
    model_manager = AsyncMock()
    model_manager.get_star_schema_model_by_fact_name.return_value = star_schema or _STAR_SCHEMA
    resolver = CubeQueryResolver()
    return CubeQuerySpecifier(version_manager, model_manager, resolver), version_manager, model_manager


def _latest_query(**kwargs) -> CubeQuery:
    defaults = dict(
        star_schema_name="Sales",
        version="latest",
        projection="default",
        filter=None,
        aggregation="default",
    )
    defaults.update(kwargs)
    return CubeQuery(**defaults)


# ---------------------------------------------------------------------------
# Tests: happy-path orchestration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_specify_calls_get_current_version_when_version_is_latest():
    specifier, vm, mm = _make_specifier()
    await specifier.specify(_latest_query())
    vm.read_version.assert_awaited_once()


@pytest.mark.asyncio
async def test_specify_calls_get_star_schema_with_name_and_resolved_version():
    specifier, vm, mm = _make_specifier()
    await specifier.specify(_latest_query())
    mm.get_star_schema_model_by_fact_name.assert_awaited_once_with(name="Sales", version=_VERSION)


@pytest.mark.asyncio
async def test_specify_returns_fully_specified_fact_query():
    specifier, vm, mm = _make_specifier()
    result = await specifier.specify(_latest_query())
    assert isinstance(result, FullySpecifiedFactQuery)


@pytest.mark.asyncio
async def test_specify_does_not_call_read_version_when_explicit_version_given():
    explicit_version = Version(
        version=3,
        model_version=3,
        reference_data_version=3,
        cube_data_version=3,
        operations_version=3,
        website_version=3,
    )
    specifier, vm, mm = _make_specifier()
    query = _latest_query(version=explicit_version)
    await specifier.specify(query)
    vm.read_version.assert_not_awaited()


@pytest.mark.asyncio
async def test_specify_uses_explicit_version_for_schema_lookup():
    explicit_version = Version(
        version=3,
        model_version=3,
        reference_data_version=3,
        cube_data_version=3,
        operations_version=3,
        website_version=3,
    )
    specifier, vm, mm = _make_specifier()
    query = _latest_query(version=explicit_version)
    await specifier.specify(query)
    mm.get_star_schema_model_by_fact_name.assert_awaited_once_with(name="Sales", version=explicit_version)


# ---------------------------------------------------------------------------
# Tests: error propagation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_specify_propagates_version_manager_error():
    specifier, vm, mm = _make_specifier()
    vm.read_version.side_effect = IOError("version store unavailable")
    with pytest.raises(IOError, match="version store unavailable"):
        await specifier.specify(_latest_query())


@pytest.mark.asyncio
async def test_specify_propagates_model_manager_error():
    specifier, vm, mm = _make_specifier()
    mm.get_star_schema_model_by_fact_name.side_effect = KeyError("Sales")
    with pytest.raises(KeyError):
        await specifier.specify(_latest_query())
