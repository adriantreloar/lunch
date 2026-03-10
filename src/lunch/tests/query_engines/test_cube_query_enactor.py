"""Tests for QueryEnactor and CubeQueryEnactor."""

import asyncio
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from src.lunch.mvcc.version import Version
from src.lunch.plans.basic_plan import BasicPlan
from src.lunch.query_engines.cube_query_enactor import CubeQueryEnactor, _enact
from src.lunch.plans.dag_plan import DagPlan
from src.lunch.query_engines.query_result import QueryResult

_VERSION = Version(
    version=7,
    model_version=1,
    reference_data_version=1,
    cube_data_version=1,
    operations_version=1,
    website_version=1,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FACT_COLUMNS = {1: [10, 20, 30]}


def _make_mock_fact_data_store(raise_=None, columns=None):
    store = MagicMock()
    if raise_ is not None:
        store.get_columns = AsyncMock(side_effect=raise_)
    else:
        store.get_columns = AsyncMock(return_value=columns if columns is not None else _FACT_COLUMNS)
    return store


def _make_mock_dimension_data_store():
    store = MagicMock()
    store.get_columns = AsyncMock(return_value={})
    return store


def _make_two_serial_dag():
    """FetchFactData -> Aggregate, returns (plan, agg_out_uuid)."""
    fetch_node_id = uuid4()
    fetch_out_uuid = uuid4()
    agg_node_id = uuid4()
    agg_out_uuid = uuid4()

    nodes = {
        fetch_node_id: BasicPlan(
            name="FetchFactData",
            inputs={"partition_id": 1, "version": 7},
            outputs={fetch_out_uuid: fetch_out_uuid},
        ),
        agg_node_id: BasicPlan(
            name="Aggregate",
            inputs={"aggregation": "sum", fetch_out_uuid: fetch_out_uuid},
            outputs={agg_out_uuid: agg_out_uuid},
        ),
    }
    plan = DagPlan(
        nodes=nodes,
        edges={(fetch_node_id, agg_node_id)},
        inputs=set(),
        outputs={agg_out_uuid},
    )
    return plan, agg_out_uuid


# ---------------------------------------------------------------------------
# DAG execution loop tests (mock dispatch table)
# ---------------------------------------------------------------------------


def test_dag_loop_runs_nodes_in_dependency_order():
    """A node whose input UUID depends on another node runs after that node."""
    call_order = []

    node_a_id = uuid4()
    out_a = uuid4()
    node_b_id = uuid4()
    out_b = uuid4()
    sentinel_a = object()
    sentinel_b = object()

    async def handler_a(node, result_registry, fds, dds):
        call_order.append("A")
        return {out_a: sentinel_a}

    async def handler_b(node, result_registry, fds, dds):
        call_order.append("B")
        assert out_a in result_registry, "B must not run before A"
        return {out_b: sentinel_b}

    nodes = {
        node_a_id: BasicPlan(name="StepA", inputs={}, outputs={out_a: out_a}),
        node_b_id: BasicPlan(name="StepB", inputs={out_a: out_a}, outputs={out_b: out_b}),
    }
    plan = DagPlan(nodes=nodes, edges={(node_a_id, node_b_id)}, inputs=set(), outputs={out_b})
    dispatch = {"StepA": handler_a, "StepB": handler_b}

    result = asyncio.run(
        _enact(
            plan=plan,
            dispatch=dispatch,
            fact_data_store=_make_mock_fact_data_store(),
            dimension_data_store=_make_mock_dimension_data_store(),
        )
    )

    assert call_order == ["A", "B"]
    assert result.data is sentinel_b


def test_dag_loop_gathers_independent_nodes_concurrently():
    """Two nodes with no shared UUID inputs are both dispatched in the same round."""
    node_a_id = uuid4()
    out_a = uuid4()
    node_b_id = uuid4()
    out_b = uuid4()
    dispatched = []

    async def handler(node, result_registry, fds, dds):
        dispatched.append(node.name)
        return {list(node.outputs.keys())[0]: object()}

    nodes = {
        node_a_id: BasicPlan(name="StepA", inputs={}, outputs={out_a: out_a}),
        node_b_id: BasicPlan(name="StepB", inputs={}, outputs={out_b: out_b}),
    }
    plan = DagPlan(nodes=nodes, edges=set(), inputs=set(), outputs={out_a, out_b})
    dispatch = {"StepA": handler, "StepB": handler}

    asyncio.run(
        _enact(
            plan=plan,
            dispatch=dispatch,
            fact_data_store=_make_mock_fact_data_store(),
            dimension_data_store=_make_mock_dimension_data_store(),
        )
    )

    assert set(dispatched) == {"StepA", "StepB"}
    assert len(dispatched) == 2


# ---------------------------------------------------------------------------
# CubeQueryEnactor.enact tests
# ---------------------------------------------------------------------------


def test_enact_two_node_dag_populates_result_and_returns_query_result():
    """FetchFactData -> Aggregate: result is propagated and wrapped in QueryResult."""
    fact_columns = {"col1": [1, 2, 3]}
    fact_store = _make_mock_fact_data_store(columns=fact_columns)
    dim_store = _make_mock_dimension_data_store()
    plan, _ = _make_two_serial_dag()

    enactor = CubeQueryEnactor(fact_data_store=fact_store, dimension_data_store=dim_store)
    result = asyncio.run(enactor.enact(plan))

    assert isinstance(result, QueryResult)
    assert result.plan is plan
    assert result.data == fact_columns
    fact_store.get_columns.assert_awaited_once()


def test_enact_returns_query_result_with_plan():
    """enact() always wraps its output in a QueryResult carrying the original plan."""
    fact_store = _make_mock_fact_data_store()
    dim_store = _make_mock_dimension_data_store()
    plan, _ = _make_two_serial_dag()

    enactor = CubeQueryEnactor(fact_data_store=fact_store, dimension_data_store=dim_store)
    result = asyncio.run(enactor.enact(plan))

    assert isinstance(result, QueryResult)
    assert result.plan is plan


def test_enact_propagates_error_from_fact_data_store():
    """IOError from FactDataStore.get_columns propagates unchanged."""
    fact_store = _make_mock_fact_data_store(raise_=IOError("disk failure"))
    dim_store = _make_mock_dimension_data_store()
    plan, _ = _make_two_serial_dag()

    enactor = CubeQueryEnactor(fact_data_store=fact_store, dimension_data_store=dim_store)
    with pytest.raises(IOError, match="disk failure"):
        asyncio.run(enactor.enact(plan))


def test_enact_key_error_from_fact_data_store_propagates():
    """KeyError from FactDataStore.get_columns propagates unchanged."""
    fact_store = _make_mock_fact_data_store(raise_=KeyError("missing partition"))
    dim_store = _make_mock_dimension_data_store()
    plan, _ = _make_two_serial_dag()

    enactor = CubeQueryEnactor(fact_data_store=fact_store, dimension_data_store=dim_store)
    with pytest.raises(KeyError):
        asyncio.run(enactor.enact(plan))


# ---------------------------------------------------------------------------
# FetchDimensionData handler tests
# ---------------------------------------------------------------------------


def _make_fetch_dimension_dag(version, attributes=None):
    """FetchDimensionData node with dimension_id, version, and optional attributes."""
    node_id = uuid4()
    out_uuid = uuid4()
    dimension = {"dimension_id": 5, "version": version}
    if attributes is not None:
        dimension["attributes"] = attributes
    nodes = {
        node_id: BasicPlan(
            name="FetchDimensionData",
            inputs={"dimension": dimension},
            outputs={out_uuid: out_uuid},
        ),
    }
    plan = DagPlan(nodes=nodes, edges=set(), inputs=set(), outputs={out_uuid})
    return plan, out_uuid


def test_fetch_dimension_data_passes_version_to_store():
    """_fetch_dimension_data calls get_columns with column_types built from attributes."""
    dim_columns = {"member_id": [1, 2, 3]}
    dim_store = MagicMock()
    dim_store.get_columns = AsyncMock(return_value=dim_columns)
    fact_store = _make_mock_fact_data_store()
    attributes = [{"id_": 10}, {"id_": 20}]
    plan, out_uuid = _make_fetch_dimension_dag(version=_VERSION, attributes=attributes)

    enactor = CubeQueryEnactor(fact_data_store=fact_store, dimension_data_store=dim_store)
    result = asyncio.run(enactor.enact(plan))

    dim_store.get_columns.assert_awaited_once_with(
        dimension_id=5,
        column_types={10: None, 20: None},
        filter=None,
        read_version=_VERSION,
    )
    assert result.data == dim_columns


def test_fetch_dimension_data_no_attributes_passes_empty_column_types():
    """_fetch_dimension_data passes column_types={} when attributes is absent."""
    dim_columns = {"member_id": [1, 2]}
    dim_store = MagicMock()
    dim_store.get_columns = AsyncMock(return_value=dim_columns)
    fact_store = _make_mock_fact_data_store()
    plan, _ = _make_fetch_dimension_dag(version=_VERSION)  # no attributes

    enactor = CubeQueryEnactor(fact_data_store=fact_store, dimension_data_store=dim_store)
    asyncio.run(enactor.enact(plan))

    dim_store.get_columns.assert_awaited_once_with(
        dimension_id=5,
        column_types={},
        filter=None,
        read_version=_VERSION,
    )


def test_fetch_dimension_data_propagates_io_error_from_dimension_store():
    """IOError from DimensionDataStore.get_columns propagates unchanged."""
    dim_store = MagicMock()
    dim_store.get_columns = AsyncMock(side_effect=IOError("dimension disk failure"))
    fact_store = _make_mock_fact_data_store()
    plan, _ = _make_fetch_dimension_dag(version=_VERSION)

    enactor = CubeQueryEnactor(fact_data_store=fact_store, dimension_data_store=dim_store)
    with pytest.raises(IOError, match="dimension disk failure"):
        asyncio.run(enactor.enact(plan))


def test_fetch_fact_data_passes_int_version_as_read_cube_data_version():
    """_fetch_fact_data forwards the int partition version as read_cube_data_version to get_columns."""
    fact_columns = {1: [10, 20, 30]}
    fact_store = _make_mock_fact_data_store(columns=fact_columns)
    dim_store = _make_mock_dimension_data_store()
    plan, _ = _make_two_serial_dag()  # node has "version": 7

    enactor = CubeQueryEnactor(fact_data_store=fact_store, dimension_data_store=dim_store)
    asyncio.run(enactor.enact(plan))

    fact_store.get_columns.assert_awaited_once_with(
        fact_id=1,
        column_types={},
        filter=None,
        read_cube_data_version=7,
    )
