import pytest
import pandas as pd
from mock import AsyncMock, Mock

from src.lunch.import_engine.dimension_import_optimiser import DimensionImportOptimiser
from src.lunch.import_engine.dimension_import_planner import DimensionImportPlanner
from src.lunch.managers.model_manager import ModelManager
from src.lunch.mvcc.version import Version
from src.lunch.plans.basic_plan import BasicPlan
from src.lunch.storage.dimension_data_store import DimensionDataStore

v0 = Version(version=0, model_version=0, reference_data_version=0,
             cube_data_version=0, operations_version=0, website_version=0)
v1 = Version(version=1, model_version=1, reference_data_version=1,
             cube_data_version=0, operations_version=0, website_version=0)

_DF = pd.DataFrame([{"foo": "a"}, {"foo": "b"}])
_DIM = {"name": "Test", "id_": 1, "attributes": [{"name": "foo", "id_": 1}], "storage": "columnar"}
_PLAN = BasicPlan(name="_import_locally_from_dataframe", inputs={}, outputs={})


@pytest.fixture
def optimiser_and_mocks():
    planner = Mock(DimensionImportPlanner)
    store = Mock(DimensionDataStore)
    store.storage_instructions = Mock(return_value={})
    manager = Mock(ModelManager)
    manager.get_dimension_by_name = AsyncMock()

    optimiser = DimensionImportOptimiser(
        dimension_import_planner=planner,
        dimension_data_store=store,
        model_manager=manager,
    )
    return optimiser, planner, store, manager


# ---------------------------------------------------------------------------
# read dimension KeyError is intentionally swallowed (first import)
# ---------------------------------------------------------------------------

async def test_read_dimension_key_error_treated_as_first_import(optimiser_and_mocks):
    optimiser, planner, store, manager = optimiser_and_mocks
    # read version: no previous data; write version: dimension exists
    manager.get_dimension_by_name.side_effect = [KeyError("Test"), _DIM]
    planner.create_local_dataframe_import_plan.return_value = _PLAN

    await optimiser.create_dataframe_import_plan("Test", _DF, v0, v1)

    call_kwargs = planner.create_local_dataframe_import_plan.call_args.kwargs
    assert call_kwargs["read_dimension"] is None


# ---------------------------------------------------------------------------
# write dimension KeyError must propagate — dimension missing from model
# ---------------------------------------------------------------------------

async def test_write_dimension_key_error_propagates(optimiser_and_mocks):
    optimiser, planner, store, manager = optimiser_and_mocks
    # Both read and write raise — only the write KeyError must propagate
    manager.get_dimension_by_name.side_effect = [KeyError("Test"), KeyError("Test")]

    with pytest.raises(KeyError):
        await optimiser.create_dataframe_import_plan("Test", _DF, v0, v1)

    planner.create_local_dataframe_import_plan.assert_not_called()


async def test_write_dimension_key_error_propagates_even_when_read_succeeds(optimiser_and_mocks):
    optimiser, planner, store, manager = optimiser_and_mocks
    # read version succeeds; write version fails
    manager.get_dimension_by_name.side_effect = [_DIM, KeyError("Test")]

    with pytest.raises(KeyError):
        await optimiser.create_dataframe_import_plan("Test", _DF, v0, v1)

    planner.create_local_dataframe_import_plan.assert_not_called()


# ---------------------------------------------------------------------------
# non-KeyError from model_manager always propagates
# ---------------------------------------------------------------------------

async def test_model_manager_io_error_on_read_propagates(optimiser_and_mocks):
    optimiser, planner, store, manager = optimiser_and_mocks
    manager.get_dimension_by_name.side_effect = IOError("storage unavailable")

    with pytest.raises(IOError):
        await optimiser.create_dataframe_import_plan("Test", _DF, v0, v1)

    planner.create_local_dataframe_import_plan.assert_not_called()


# ---------------------------------------------------------------------------
# planner failure propagates
# ---------------------------------------------------------------------------

async def test_planner_error_propagates(optimiser_and_mocks):
    optimiser, planner, store, manager = optimiser_and_mocks
    manager.get_dimension_by_name.return_value = _DIM
    planner.create_local_dataframe_import_plan.side_effect = ValueError("cannot plan")

    with pytest.raises(ValueError):
        await optimiser.create_dataframe_import_plan("Test", _DF, v0, v1)
