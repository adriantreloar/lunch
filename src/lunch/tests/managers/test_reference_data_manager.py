import pandas as pd
import pytest
from mock import AsyncMock, Mock

from src.lunch.import_engine.dimension_import_enactor import DimensionImportEnactor
from src.lunch.import_engine.dimension_import_optimiser import DimensionImportOptimiser
from src.lunch.managers.reference_data_manager import ReferenceDataManager
from src.lunch.mvcc.version import Version
from src.lunch.plans.basic_plan import BasicPlan
from src.lunch.storage.dimension_data_store import DimensionDataStore
from src.lunch.storage.reference_data_store import ReferenceDataStore

v0 = Version(
    version=0, model_version=0, reference_data_version=0, cube_data_version=0, operations_version=0, website_version=0
)
v1 = Version(
    version=1, model_version=1, reference_data_version=1, cube_data_version=0, operations_version=0, website_version=0
)

_DF = pd.DataFrame([{"foo": "a"}, {"foo": "b"}])


@pytest.fixture
def manager_and_mocks():
    dim_store = Mock(DimensionDataStore)
    ref_store = Mock(ReferenceDataStore)
    ref_store.dimension_data_store = dim_store

    optimiser = Mock(DimensionImportOptimiser)
    optimiser.create_dataframe_import_plan = AsyncMock()

    enactor = Mock(DimensionImportEnactor)
    enactor.enact_plan = AsyncMock()

    manager = ReferenceDataManager(
        reference_data_store=ref_store,
        dimension_import_optimiser=optimiser,
        dimension_import_enactor=enactor,
    )
    return manager, optimiser, enactor


# ---------------------------------------------------------------------------
# optimiser failure
# ---------------------------------------------------------------------------


async def test_optimiser_key_error_propagates_and_enactor_not_called(manager_and_mocks):
    manager, optimiser, enactor = manager_and_mocks
    optimiser.create_dataframe_import_plan.side_effect = KeyError("NoSuchDimension")

    with pytest.raises(KeyError):
        await manager.update_dimension_from_dataframe(
            name="NoSuchDimension",
            data=_DF,
            read_version=v0,
            write_version=v1,
        )

    enactor.enact_plan.assert_not_called()


async def test_optimiser_runtime_error_propagates(manager_and_mocks):
    manager, optimiser, enactor = manager_and_mocks
    optimiser.create_dataframe_import_plan.side_effect = RuntimeError("optimiser failed")

    with pytest.raises(RuntimeError):
        await manager.update_dimension_from_dataframe(
            name="Test",
            data=_DF,
            read_version=v0,
            write_version=v1,
        )

    enactor.enact_plan.assert_not_called()


# ---------------------------------------------------------------------------
# enactor failure
# ---------------------------------------------------------------------------


async def test_enactor_io_error_propagates(manager_and_mocks):
    manager, optimiser, enactor = manager_and_mocks
    optimiser.create_dataframe_import_plan.return_value = Mock(BasicPlan)
    enactor.enact_plan.side_effect = IOError("disk full")

    with pytest.raises(IOError):
        await manager.update_dimension_from_dataframe(
            name="Test",
            data=_DF,
            read_version=v0,
            write_version=v1,
        )


async def test_enactor_value_error_propagates(manager_and_mocks):
    manager, optimiser, enactor = manager_and_mocks
    optimiser.create_dataframe_import_plan.return_value = Mock(BasicPlan)
    enactor.enact_plan.side_effect = ValueError("unknown plan type")

    with pytest.raises(ValueError):
        await manager.update_dimension_from_dataframe(
            name="Test",
            data=_DF,
            read_version=v0,
            write_version=v1,
        )
