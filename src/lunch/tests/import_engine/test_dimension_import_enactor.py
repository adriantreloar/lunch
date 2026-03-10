from uuid import uuid1

import pandas as pd
import pytest
from mock import AsyncMock, Mock

from src.lunch.import_engine.dimension_import_enactor import DimensionImportEnactor
from src.lunch.mvcc.version import Version
from src.lunch.plans.basic_plan import BasicPlan
from src.lunch.plans.dag_plan import DagPlan
from src.lunch.storage.dimension_data_store import DimensionDataStore

v0 = Version(
    version=0, model_version=0, reference_data_version=0, cube_data_version=0, operations_version=0, website_version=0
)
v1 = Version(
    version=1, model_version=1, reference_data_version=1, cube_data_version=0, operations_version=0, website_version=0
)

_DF = pd.DataFrame([{"foo": "a"}, {"foo": "b"}])

_WRITE_DIM = {"name": "Test", "id_": 1, "attributes": [{"name": "foo", "id_": 1}]}
_READ_DIM = {"name": "Test", "id_": 1, "attributes": [{"name": "foo", "id_": 1}]}

_LOCAL_PLAN_NO_READ = BasicPlan(
    name="_import_locally_from_dataframe",
    inputs={
        "read_dimension": None,
        "write_dimension": _WRITE_DIM,
        "read_filter": {},
        "merge_key": [0],
    },
    outputs={},
)

_LOCAL_PLAN_WITH_READ = BasicPlan(
    name="_import_locally_from_dataframe",
    inputs={
        "read_dimension": _READ_DIM,
        "write_dimension": _WRITE_DIM,
        "read_filter": {},
        "merge_key": [0],
    },
    outputs={},
)


@pytest.fixture
def enactor_and_store():
    store = Mock(DimensionDataStore)
    store.get_columns = AsyncMock()
    store.put = AsyncMock()
    return DimensionImportEnactor(), store


# ---------------------------------------------------------------------------
# unknown plan type
# ---------------------------------------------------------------------------


async def test_unknown_plan_name_raises_value_error(enactor_and_store):
    enactor, store = enactor_and_store
    bad_plan = BasicPlan(name="some_unknown_function", inputs={}, outputs={})

    with pytest.raises(ValueError):
        await enactor.enact_plan(
            import_plan=bad_plan,
            data=_DF,
            read_version=v0,
            write_version=v1,
            dimension_data_store=store,
        )

    store.put.assert_not_called()


# ---------------------------------------------------------------------------
# storage write failure
# ---------------------------------------------------------------------------


async def test_put_failure_propagates(enactor_and_store):
    enactor, store = enactor_and_store
    store.put.side_effect = IOError("disk full")

    with pytest.raises(IOError):
        await enactor.enact_plan(
            import_plan=_LOCAL_PLAN_NO_READ,
            data=_DF,
            read_version=v0,
            write_version=v1,
            dimension_data_store=store,
        )


# ---------------------------------------------------------------------------
# get_columns failure: KeyError is the expected first-import path
# ---------------------------------------------------------------------------


async def test_get_columns_key_error_treated_as_first_import(enactor_and_store):
    enactor, store = enactor_and_store
    store.get_columns.side_effect = KeyError("no data yet")
    store.put.return_value = None

    # Should not raise — KeyError on get_columns means first import
    await enactor.enact_plan(
        import_plan=_LOCAL_PLAN_WITH_READ,
        data=_DF,
        read_version=v0,
        write_version=v1,
        dimension_data_store=store,
    )

    store.put.assert_called_once()


# ---------------------------------------------------------------------------
# get_columns failure: non-KeyError must propagate, not be mistaken for
# a first import
# ---------------------------------------------------------------------------


async def test_get_columns_io_error_propagates_and_is_not_swallowed(enactor_and_store):
    enactor, store = enactor_and_store
    store.get_columns.side_effect = IOError("network error")

    with pytest.raises(IOError):
        await enactor.enact_plan(
            import_plan=_LOCAL_PLAN_WITH_READ,
            data=_DF,
            read_version=v0,
            write_version=v1,
            dimension_data_store=store,
        )

    store.put.assert_not_called()


# ---------------------------------------------------------------------------
# DagPlan: DAG execution
# ---------------------------------------------------------------------------


async def test_dag_plan_executes_all_nodes(enactor_and_store):
    enactor, store = enactor_and_store
    store.get_columns.side_effect = KeyError("no data")
    store.put.return_value = None

    dim_a = {"name": "A", "id_": 1, "attributes": [{"name": "foo", "id_": 1}]}
    dim_b = {"name": "B", "id_": 2, "attributes": [{"name": "bar", "id_": 1}]}

    node_a = uuid1()
    node_b = uuid1()
    dag = DagPlan(
        nodes={
            node_a: BasicPlan(
                name="_import_locally_from_dataframe",
                inputs={"read_dimension": None, "write_dimension": dim_a, "read_filter": {}, "merge_key": [0]},
                outputs={},
            ),
            node_b: BasicPlan(
                name="_import_locally_from_dataframe",
                inputs={"read_dimension": None, "write_dimension": dim_b, "read_filter": {}, "merge_key": [0]},
                outputs={},
            ),
        },
        edges=set(),
        inputs=set(),
        outputs=set(),
    )

    await enactor.enact_plan(
        import_plan=dag,
        data=_DF,
        read_version=v0,
        write_version=v1,
        dimension_data_store=store,
    )

    assert store.put.call_count == 2


async def test_dag_plan_uuid_output_resolved_as_input_to_later_node(enactor_and_store):
    enactor, store = enactor_and_store
    # Node A writes dim_a and produces it as a UUID-keyed output.
    # Node B references that UUID as its read_dimension input.
    dim_a = {"name": "A", "id_": 1, "attributes": [{"name": "foo", "id_": 1}]}
    dim_b = {"name": "B", "id_": 2, "attributes": [{"name": "bar", "id_": 1}]}
    handle = uuid1()

    node_a = uuid1()
    node_b = uuid1()
    dag = DagPlan(
        nodes={
            node_a: BasicPlan(
                name="_import_locally_from_dataframe",
                inputs={"read_dimension": None, "write_dimension": dim_a, "read_filter": {}, "merge_key": [0]},
                outputs={"write_dimension": handle},
            ),
            node_b: BasicPlan(
                name="_import_locally_from_dataframe",
                inputs={"read_dimension": handle, "write_dimension": dim_b, "read_filter": {}, "merge_key": [0]},
                outputs={},
            ),
        },
        edges={(node_b, node_a)},
        inputs=set(),
        outputs=set(),
    )

    # get_columns raises KeyError (first-import path) for both nodes
    store.get_columns.side_effect = KeyError("no data")
    store.put.return_value = None

    await enactor.enact_plan(
        import_plan=dag,
        data=_DF,
        read_version=v0,
        write_version=v1,
        dimension_data_store=store,
    )

    # Node B resolved read_dimension to dim_a (not None), so get_columns must have been called once.
    store.get_columns.assert_called_once()
    assert store.put.call_count == 2


async def test_dag_plan_unknown_node_raises_value_error(enactor_and_store):
    enactor, store = enactor_and_store
    bad_step = BasicPlan(name="unknown_function", inputs={}, outputs={})
    node_id = uuid1()
    dag = DagPlan(nodes={node_id: bad_step}, edges=set(), inputs=set(), outputs=set())

    with pytest.raises(ValueError):
        await enactor.enact_plan(
            import_plan=dag,
            data=_DF,
            read_version=v0,
            write_version=v1,
            dimension_data_store=store,
        )
