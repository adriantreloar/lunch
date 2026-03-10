"""Tests for CubeQueryPlanner."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.lunch.model.fact import Fact, FactStorage, _FactDimensionsMetadata, _FactMeasuresMetadata
from src.lunch.model.star_schema import StarSchema
from src.lunch.mvcc.version import Version
from src.lunch.queries.fully_specified_fact_query import FullySpecifiedFactQuery
from src.lunch.query_engines.cube_query_dag_builder import CubeQueryDagBuilder
from src.lunch.query_engines.cube_query_planner import CubeQueryPlanner
from src.lunch.plans.dag_plan import DagPlan

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
    version=7,
    model_version=1,
    reference_data_version=1,
    cube_data_version=1,
    operations_version=1,
    website_version=1,
)
_QUERY = FullySpecifiedFactQuery(
    star_schema=_STAR_SCHEMA,
    version=_VERSION,
    dimensions=[],
    measures=[],
    filters=[],
    aggregations=["sum"],
)
_PARTITION_MANIFEST = {42: 7}
_DAG_PLAN = DagPlan(nodes={}, edges=set(), inputs=set(), outputs=set())


def _make_mock_fact_data_store(manifest=None, raise_=None):
    store = MagicMock()
    if raise_ is not None:
        store.get_partition_manifest = AsyncMock(side_effect=raise_)
    else:
        store.get_partition_manifest = AsyncMock(return_value=manifest if manifest is not None else _PARTITION_MANIFEST)
    return store


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_plan_calls_fact_data_store_with_version():
    """plan() awaits get_partition_manifest with the query version."""
    store = _make_mock_fact_data_store()

    with patch.object(CubeQueryDagBuilder, "build", return_value=_DAG_PLAN):
        planner = CubeQueryPlanner(fact_data_store=store, cube_query_dag_builder=CubeQueryDagBuilder())
        result = asyncio.run(planner.plan(_QUERY))

    store.get_partition_manifest.assert_awaited_once_with(version=_VERSION)
    assert result is _DAG_PLAN


def test_plan_passes_manifest_to_builder():
    """plan() passes the fetched partition manifest to CubeQueryDagBuilder.build()."""
    store = _make_mock_fact_data_store(manifest=_PARTITION_MANIFEST)

    with patch.object(CubeQueryDagBuilder, "build", return_value=_DAG_PLAN) as mock_build:
        planner = CubeQueryPlanner(fact_data_store=store, cube_query_dag_builder=CubeQueryDagBuilder())
        asyncio.run(planner.plan(_QUERY))

    mock_build.assert_called_once_with(query=_QUERY, partitions=_PARTITION_MANIFEST)


def test_plan_returns_dag_plan_from_builder():
    """plan() returns whatever CubeQueryDagBuilder.build() produces."""
    store = _make_mock_fact_data_store()

    with patch.object(CubeQueryDagBuilder, "build", return_value=_DAG_PLAN):
        planner = CubeQueryPlanner(fact_data_store=store, cube_query_dag_builder=CubeQueryDagBuilder())
        result = asyncio.run(planner.plan(_QUERY))

    assert result is _DAG_PLAN


def test_plan_propagates_error_from_fact_data_store():
    """Errors from get_partition_manifest propagate to the caller unchanged."""
    store = _make_mock_fact_data_store(raise_=IOError("disk failure"))

    planner = CubeQueryPlanner(fact_data_store=store, cube_query_dag_builder=CubeQueryDagBuilder())
    with pytest.raises(IOError, match="disk failure"):
        asyncio.run(planner.plan(_QUERY))
