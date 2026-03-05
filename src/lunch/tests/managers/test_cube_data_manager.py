import pandas as pd
import pytest
from mock import AsyncMock, Mock

from src.lunch.import_engine.fact_import_enactor import FactImportEnactor
from src.lunch.import_engine.fact_import_optimiser import FactImportOptimiser
from src.lunch.managers.cube_data_manager import CubeDataManager
from src.lunch.managers.model_manager import ModelManager
from src.lunch.mvcc.version import Version
from src.lunch.plans.basic_plan import BasicPlan
from src.lunch.storage.fact_data_store import FactDataStore

v0 = Version(version=0, model_version=0, reference_data_version=0,
             cube_data_version=0, operations_version=0, website_version=0)
v1 = Version(version=1, model_version=1, reference_data_version=1,
             cube_data_version=0, operations_version=0, website_version=0)

_DF = pd.DataFrame([{"dept": 1, "sales": 10.0}])
_PLAN = Mock(BasicPlan)


@pytest.fixture
def manager_and_mocks():
    model_manager = Mock(ModelManager)
    fact_data_store = Mock(FactDataStore)
    optimiser = Mock(FactImportOptimiser)
    enactor = Mock(FactImportEnactor)
    enactor.enact_plan = AsyncMock()

    manager = CubeDataManager(
        model_manager=model_manager,
        fact_data_store=fact_data_store,
        fact_import_optimiser=optimiser,
        fact_import_enactor=enactor,
    )
    return manager, enactor, fact_data_store


# ---------------------------------------------------------------------------
# enactor failure
# ---------------------------------------------------------------------------

async def test_enactor_io_error_propagates(manager_and_mocks):
    manager, enactor, _ = manager_and_mocks
    enactor.enact_plan.side_effect = IOError("disk full")

    with pytest.raises(IOError):
        await manager.append_fact_from_dataframe(
            plan=_PLAN, source_data=_DF,
            read_version=v0, write_version=v1,
        )


async def test_enactor_value_error_propagates(manager_and_mocks):
    manager, enactor, _ = manager_and_mocks
    enactor.enact_plan.side_effect = ValueError("unknown plan type")

    with pytest.raises(ValueError):
        await manager.append_fact_from_dataframe(
            plan=_PLAN, source_data=_DF,
            read_version=v0, write_version=v1,
        )


# ---------------------------------------------------------------------------
# enactor receives the correct arguments
# ---------------------------------------------------------------------------

async def test_enactor_called_with_plan_data_versions_and_store(manager_and_mocks):
    manager, enactor, fact_data_store = manager_and_mocks
    enactor.enact_plan.return_value = None

    await manager.append_fact_from_dataframe(
        plan=_PLAN, source_data=_DF,
        read_version=v0, write_version=v1,
    )

    enactor.enact_plan.assert_called_once_with(
        _PLAN, _DF, v0, v1, fact_data_store,
    )
