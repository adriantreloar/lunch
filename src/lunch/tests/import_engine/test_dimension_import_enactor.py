import pytest
import pandas as pd
from mock import AsyncMock, Mock

from src.lunch.import_engine.dimension_import_enactor import DimensionImportEnactor
from src.lunch.mvcc.version import Version
from src.lunch.plans.basic_plan import BasicPlan
from src.lunch.storage.dimension_data_store import DimensionDataStore

v0 = Version(version=0, model_version=0, reference_data_version=0,
             cube_data_version=0, operations_version=0, website_version=0)
v1 = Version(version=1, model_version=1, reference_data_version=1,
             cube_data_version=0, operations_version=0, website_version=0)

_DF = pd.DataFrame([{"foo": "a"}, {"foo": "b"}])

_WRITE_DIM = {"name": "Test", "id_": 1, "attributes": [{"name": "foo", "id_": 1}]}
_READ_DIM  = {"name": "Test", "id_": 1, "attributes": [{"name": "foo", "id_": 1}]}

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
            import_plan=bad_plan, data=_DF,
            read_version=v0, write_version=v1,
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
            import_plan=_LOCAL_PLAN_NO_READ, data=_DF,
            read_version=v0, write_version=v1,
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
        import_plan=_LOCAL_PLAN_WITH_READ, data=_DF,
        read_version=v0, write_version=v1,
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
            import_plan=_LOCAL_PLAN_WITH_READ, data=_DF,
            read_version=v0, write_version=v1,
            dimension_data_store=store,
        )

    store.put.assert_not_called()
