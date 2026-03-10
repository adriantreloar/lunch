"""Tests for QueryEngine and CubeQueryEngine."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.lunch.queries.cube_query import CubeQuery
from src.lunch.query_engines.cube_query_engine import CubeQueryEngine
from src.lunch.plans.dag_plan import DagPlan
from src.lunch.query_engines.query_result import QueryResult

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_CUBE_QUERY = CubeQuery(
    star_schema_name="Sales",
    version="latest",
    projection="default",
    filter=None,
    aggregation="default",
)

_FULLY_SPECIFIED = MagicMock(name="FullySpecifiedFactQuery")
_DAG_PLAN = DagPlan(nodes={}, edges=set(), inputs=set(), outputs=set())
_QUERY_RESULT = QueryResult(data={"col": [1, 2, 3]}, query=None, plan=_DAG_PLAN)


def _make_mocks(specifier_raise=None, planner_raise=None, enactor_raise=None):
    specifier = MagicMock()
    planner = MagicMock()
    enactor = MagicMock()

    specifier.specify = AsyncMock(
        side_effect=specifier_raise if specifier_raise else None,
        return_value=_FULLY_SPECIFIED,
    )
    planner.plan = AsyncMock(
        side_effect=planner_raise if planner_raise else None,
        return_value=_DAG_PLAN,
    )
    enactor.enact = AsyncMock(
        side_effect=enactor_raise if enactor_raise else None,
        return_value=_QUERY_RESULT,
    )
    return specifier, planner, enactor


# ---------------------------------------------------------------------------
# Happy-path wiring tests
# ---------------------------------------------------------------------------


def test_query_calls_specifier_with_original_query():
    """query() passes the original CubeQuery to specifier.specify."""
    specifier, planner, enactor = _make_mocks()
    engine = CubeQueryEngine(specifier=specifier, planner=planner, enactor=enactor)

    asyncio.run(engine.query(_CUBE_QUERY))

    specifier.specify.assert_awaited_once_with(_CUBE_QUERY)


def test_query_calls_planner_with_fully_specified_query():
    """query() passes the FullySpecifiedFactQuery from specifier to planner.plan."""
    specifier, planner, enactor = _make_mocks()
    engine = CubeQueryEngine(specifier=specifier, planner=planner, enactor=enactor)

    asyncio.run(engine.query(_CUBE_QUERY))

    planner.plan.assert_awaited_once_with(_FULLY_SPECIFIED)


def test_query_calls_enactor_with_dag_plan():
    """query() passes the DagPlan from planner to enactor.enact."""
    specifier, planner, enactor = _make_mocks()
    engine = CubeQueryEngine(specifier=specifier, planner=planner, enactor=enactor)

    asyncio.run(engine.query(_CUBE_QUERY))

    enactor.enact.assert_awaited_once_with(_DAG_PLAN)


def test_query_returns_query_result_from_enactor():
    """query() returns the QueryResult produced by the enactor."""
    specifier, planner, enactor = _make_mocks()
    engine = CubeQueryEngine(specifier=specifier, planner=planner, enactor=enactor)

    result = asyncio.run(engine.query(_CUBE_QUERY))

    assert result is _QUERY_RESULT


# ---------------------------------------------------------------------------
# Error propagation tests
# ---------------------------------------------------------------------------


def test_query_propagates_error_from_specifier():
    """Errors from specifier.specify propagate to the caller unchanged."""
    specifier, planner, enactor = _make_mocks(specifier_raise=KeyError("unknown schema"))
    engine = CubeQueryEngine(specifier=specifier, planner=planner, enactor=enactor)

    with pytest.raises(KeyError, match="unknown schema"):
        asyncio.run(engine.query(_CUBE_QUERY))


def test_query_propagates_error_from_planner():
    """Errors from planner.plan propagate to the caller unchanged."""
    specifier, planner, enactor = _make_mocks(planner_raise=IOError("storage error"))
    engine = CubeQueryEngine(specifier=specifier, planner=planner, enactor=enactor)

    with pytest.raises(IOError, match="storage error"):
        asyncio.run(engine.query(_CUBE_QUERY))


def test_query_propagates_error_from_enactor():
    """Errors from enactor.enact propagate to the caller unchanged."""
    specifier, planner, enactor = _make_mocks(enactor_raise=RuntimeError("enact failed"))
    engine = CubeQueryEngine(specifier=specifier, planner=planner, enactor=enactor)

    with pytest.raises(RuntimeError, match="enact failed"):
        asyncio.run(engine.query(_CUBE_QUERY))
