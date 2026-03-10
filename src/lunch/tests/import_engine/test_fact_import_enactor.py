from uuid import uuid1

import pandas as pd
import pytest
from mock import AsyncMock, Mock

from src.lunch.import_engine.fact_import_enactor import FactImportEnactor
from src.lunch.model.fact import Fact
from src.lunch.mvcc.version import Version
from src.lunch.plans.basic_plan import BasicPlan
from src.lunch.plans.serial_plan import SerialPlan
from src.lunch.storage.fact_data_store import FactDataStore

v0 = Version(
    version=0, model_version=0, reference_data_version=0, cube_data_version=0, operations_version=0, website_version=0
)
v1 = Version(
    version=1, model_version=1, reference_data_version=1, cube_data_version=1, operations_version=0, website_version=0
)

_DF = pd.DataFrame([{"dept": 1, "sales": 10.0}])

_read_fact = Mock(Fact)
_read_fact.fact_id = 1
_write_fact = Mock(Fact)
_write_fact.fact_id = 1

_PLAN = BasicPlan(
    name="_import_fact_append_locally_from_dataframe",
    inputs={
        "read_fact": _read_fact,
        "column_id_mapping": {"dept": 3, "sales": 1},
        "merge_key": [1],
        "read_filter": None,
    },
    outputs={"write_fact": _write_fact},
)


@pytest.fixture
def enactor_and_store():
    store = Mock(FactDataStore)
    store.get_columns = AsyncMock()
    store.put = AsyncMock()
    return FactImportEnactor(), store


# ---------------------------------------------------------------------------
# unknown plan type
# ---------------------------------------------------------------------------


async def test_unknown_plan_name_raises_value_error(enactor_and_store):
    enactor, store = enactor_and_store
    bad_plan = BasicPlan(name="some_unknown_function", inputs={}, outputs={})

    with pytest.raises(ValueError):
        await enactor.enact_plan(
            append_plan=bad_plan,
            data=_DF,
            read_version=v0,
            write_version=v1,
            fact_data_store=store,
        )

    store.put.assert_not_called()


# ---------------------------------------------------------------------------
# storage write failure
# ---------------------------------------------------------------------------


async def test_put_failure_propagates(enactor_and_store):
    enactor, store = enactor_and_store
    store.get_columns.side_effect = KeyError("no data yet")
    store.put.side_effect = IOError("disk full")

    with pytest.raises(IOError):
        await enactor.enact_plan(
            append_plan=_PLAN,
            data=_DF,
            read_version=v0,
            write_version=v1,
            fact_data_store=store,
        )


# ---------------------------------------------------------------------------
# get_columns: KeyError is the first-import path
# ---------------------------------------------------------------------------


async def test_get_columns_key_error_treated_as_first_import(enactor_and_store):
    enactor, store = enactor_and_store
    store.get_columns.side_effect = KeyError("no data yet")
    store.put.return_value = None

    # Must not raise — KeyError on get_columns means first import
    await enactor.enact_plan(
        append_plan=_PLAN,
        data=_DF,
        read_version=v0,
        write_version=v1,
        fact_data_store=store,
    )

    store.put.assert_called_once()


# ---------------------------------------------------------------------------
# get_columns: non-KeyError must propagate, not be swallowed
# ---------------------------------------------------------------------------


async def test_get_columns_io_error_propagates_and_is_not_swallowed(enactor_and_store):
    enactor, store = enactor_and_store
    store.get_columns.side_effect = IOError("network error")

    with pytest.raises(IOError):
        await enactor.enact_plan(
            append_plan=_PLAN,
            data=_DF,
            read_version=v0,
            write_version=v1,
            fact_data_store=store,
        )

    store.put.assert_not_called()


# ---------------------------------------------------------------------------
# update path: existing data is merged before put
# ---------------------------------------------------------------------------


async def test_existing_data_triggers_merge_and_put(enactor_and_store):
    enactor, store = enactor_and_store
    # Existing data keyed by storage column ids (matching column_id_mapping values)
    store.get_columns.return_value = {3: [99], 1: [5.0]}
    store.put.return_value = None

    await enactor.enact_plan(
        append_plan=_PLAN,
        data=_DF,
        read_version=v0,
        write_version=v1,
        fact_data_store=store,
    )

    store.get_columns.assert_called_once()
    store.put.assert_called_once()


# ---------------------------------------------------------------------------
# SerialPlan: sequential execution
# ---------------------------------------------------------------------------


async def test_serial_plan_executes_all_steps(enactor_and_store):
    enactor, store = enactor_and_store
    store.get_columns.side_effect = KeyError("no data")
    store.put.return_value = None

    write_fact2 = Mock(Fact)
    write_fact2.fact_id = 2

    plan2 = BasicPlan(
        name="_import_fact_append_locally_from_dataframe",
        inputs={
            "read_fact": _read_fact,
            "column_id_mapping": {"dept": 3, "sales": 1},
            "merge_key": [1],
            "read_filter": None,
        },
        outputs={"write_fact": write_fact2},
    )
    serial = SerialPlan(steps=[_PLAN, plan2])

    await enactor.enact_plan(
        append_plan=serial,
        data=_DF,
        read_version=v0,
        write_version=v1,
        fact_data_store=store,
    )

    assert store.put.call_count == 2

async def test_serial_plan_unknown_step_raises_value_error(enactor_and_store):
    enactor, store = enactor_and_store
    bad_step = BasicPlan(name="unknown_function", inputs={}, outputs={})
    serial = SerialPlan(steps=[bad_step])

    with pytest.raises(ValueError):
        await enactor.enact_plan(
            append_plan=serial,
            data=_DF,
            read_version=v0,
            write_version=v1,
            fact_data_store=store,
        )
