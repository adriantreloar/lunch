"""End-to-end integration test for the fact import pipeline.

Mirrors the flow in examples/insert_fact_data.py but uses StringIO persistors
so no files are written to disk.  After importing three rows of Sales fact data
the test reads the stored columnar data back and asserts on the exact values.
"""

from pathlib import Path

import pandas as pd
import pytest

from src.lunch.globals.global_state import GlobalState
from src.lunch.import_engine.fact_append_planner import FactAppendPlanner
from src.lunch.import_engine.fact_import_enactor import FactImportEnactor
from src.lunch.import_engine.fact_import_optimiser import FactImportOptimiser
from src.lunch.managers.cube_data_manager import CubeDataManager
from src.lunch.managers.model_manager import ModelManager
from src.lunch.model.dimension.dimension_comparer import DimensionComparer
from src.lunch.model.dimension.dimension_reference_validator import DimensionReferenceValidator
from src.lunch.model.dimension.dimension_structure_validator import DimensionStructureValidator
from src.lunch.model.dimension.dimension_transformer import DimensionTransformer
from src.lunch.model.fact import Fact, FactStorage
from src.lunch.model.old_fact.fact_comparer import FactComparer
from src.lunch.model.old_fact.fact_reference_validator import FactReferenceValidator
from src.lunch.model.table_metadata import TableMetadata
from src.lunch.mvcc.version import Version
from src.lunch.storage.cache.null_fact_data_cache import NullFactDataCache
from src.lunch.storage.cache.null_model_cache import NullModelCache
from src.lunch.storage.fact_data_store import FactDataStore
from src.lunch.storage.model_store import ModelStore
from src.lunch.storage.persistence.stringio_columnar_fact_data_persistor import (
    StringIOColumnarFactDataPersistor,
)
from src.lunch.storage.persistence.stringio_model_persistor import StringIOModelPersistor
from src.lunch.storage.serialization.columnar_fact_data_serializer import ColumnarFactDataSerializer
from src.lunch.storage.serialization.yaml_model_serializer import YamlModelSerializer
from src.lunch.storage.transformers.dimension_model_index_transformer import (
    DimensionModelIndexTransformer,
)
from src.lunch.storage.transformers.fact_model_index_transformer import FactModelIndexTransformer

# ── fake root path (no files will actually be created) ───────────────────────
_DIR = Path("/test/fake")

# ── versions ─────────────────────────────────────────────────────────────────
# v0 → v1 : save Department + Time dimensions  (model_version 0 → 1)
# v1 → v2 : save Sales fact                    (model_version 1 → 2)
# v2 → v3 : import fact data                   (cube_data_version 0 → 1)

_v0 = Version(
    version=0, model_version=0, reference_data_version=0, cube_data_version=0, operations_version=0, website_version=0
)
_v1 = Version(
    version=1, model_version=1, reference_data_version=0, cube_data_version=0, operations_version=0, website_version=0
)
_v2 = Version(
    version=2, model_version=2, reference_data_version=0, cube_data_version=0, operations_version=0, website_version=0
)
_v3 = Version(
    version=3, model_version=2, reference_data_version=0, cube_data_version=1, operations_version=0, website_version=0
)

# ── source data (mirrors insert_fact_data.py) ─────────────────────────────────
_DATA = [
    {"department_id": 1, "thing 2": 10, "sales value": 10.10},
    {"department_id": 1, "thing 2": 10, "sales value": 10.10},
    {"department_id": 1, "thing 2": 10, "sales value": 10.10},
]
_DF = pd.DataFrame(data=_DATA)

_COLUMN_MAPPING = [
    {"source": ["department_id"], "target": ["Department", "id_"]},
    {"source": ["thing 2"], "target": ["Time", "thing 2"]},
    {"source": ["sales value"], "measure target": ["measures", "sales"]},
]

_SOURCE_METADATA = TableMetadata(
    column_names=list(_DF.columns),
    column_types=list(_DF.dtypes),
    length=_DF.shape[0],
)


# ── fixture ───────────────────────────────────────────────────────────────────


@pytest.fixture
def stack():
    """Build a fully in-memory fact import stack."""
    model_persistor = StringIOModelPersistor(directory=_DIR / "model")
    model_serializer = YamlModelSerializer(persistor=model_persistor)
    model_store = ModelStore(
        dimension_comparer=DimensionComparer(),
        dimension_transformer=DimensionTransformer(),
        dimension_index_transformer=DimensionModelIndexTransformer(),
        fact_index_transformer=FactModelIndexTransformer(),
        serializer=model_serializer,
        cache=NullModelCache(),
    )
    model_mgr = ModelManager(
        dimension_structure_validator=DimensionStructureValidator(),
        dimension_comparer=DimensionComparer(),
        dimension_reference_validator=DimensionReferenceValidator(),
        dimension_transformer=DimensionTransformer(),
        fact_comparer=FactComparer(),
        fact_reference_validator=FactReferenceValidator(),
        storage=model_store,
        global_state=GlobalState(),
    )

    fact_persistor = StringIOColumnarFactDataPersistor(directory=_DIR / "fact")
    fact_serializer = ColumnarFactDataSerializer(persistor=fact_persistor)
    fact_store = FactDataStore(serializer=fact_serializer, cache=NullFactDataCache())

    planner = FactAppendPlanner()
    optimiser = FactImportOptimiser(
        fact_append_planner=planner,
        fact_data_store=fact_store,
        model_manager=model_mgr,
    )
    cube_data_mgr = CubeDataManager(
        model_manager=model_mgr,
        fact_data_store=fact_store,
        fact_import_optimiser=optimiser,
        fact_import_enactor=FactImportEnactor(),
    )

    return model_mgr, fact_store, optimiser, cube_data_mgr


# ── helpers ───────────────────────────────────────────────────────────────────


async def _set_up_model(model_mgr: ModelManager) -> None:
    """Save Department + Time dimensions, then the Sales fact."""
    d_department = {"name": "Department", "attributes": [{"name": "thing1"}]}
    d_time = {"name": "Time", "attributes": [{"name": "thing2"}]}

    await model_mgr.update_model(
        dimensions=[d_department, d_time],
        facts=[],
        read_version=_v0,
        write_version=_v1,
    )

    # Department column_id=0 is falsy → fill_default_column_ids assigns it
    # max(0,2)+1 = 3.  Time stays at column_id=2.
    f_sales = Fact(
        name="Sales",
        dimensions=[
            {"name": "Department", "column_id": 0, "view_order": 1, "dimension_name": "Department", "dimension_id": 0},
            {"name": "Time", "column_id": 2, "view_order": 2, "dimension_name": "Time", "dimension_id": 0},
        ],
        measures=[{"name": "sales", "measure_id": 1, "type": "decimal", "precision": 2}],
        storage=FactStorage(index_columns=[1], data_columns=[2, 0]),
    )

    await model_mgr.update_model(
        dimensions=[],
        facts=[f_sales],
        read_version=_v1,
        write_version=_v2,
    )


# ── test ──────────────────────────────────────────────────────────────────────


async def test_fact_data_stored_correctly_after_import(stack):
    """
    After running the full planner → enactor pipeline the columnar fact data
    stored in the in-memory persistor must match the source DataFrame rows.

    Column-id mapping (determined by the model):
      department_id  → column_id 3  (Department, fill_default_column_ids: 0→3)
      thing 2        → column_id 2  (Time)
      sales value    → column_id 1  (measure sales, measure_id=1)

    Values are serialised as str(value)+"\\n" per row, so the raw strings
    returned by get_columns include the trailing newline.
    """
    model_mgr, fact_store, optimiser, cube_data_mgr = stack

    await _set_up_model(model_mgr)

    # Both read- and write-version share model_version=2 so both resolve Sales.
    read_model = await model_mgr.get_star_schema_model_by_fact_name(name="Sales", version=_v2)
    write_model = await model_mgr.get_star_schema_model_by_fact_name(name="Sales", version=_v3)

    plan = await optimiser.create_dataframe_append_plan(
        read_version_target_model=read_model,
        write_version_target_model=write_model,
        source_metadata=_SOURCE_METADATA,
        column_mapping=_COLUMN_MAPPING,
        read_version=_v2,
        write_version=_v3,
    )

    await cube_data_mgr.append_fact_from_dataframe(
        plan=plan,
        source_data=_DF,
        read_version=_v2,
        write_version=_v3,
    )

    # fact_id = 1 (first and only fact saved, assigned by model_store)
    columns = await fact_store.get_columns(
        read_version=_v3,
        fact_id=1,
        column_types={1: str, 2: str, 3: str},
        filter=None,
    )

    # Values written as str(val)+"\n" by the columnar fact serializer
    assert list(columns[3]) == ["1\n", "1\n", "1\n"]  # department_id
    assert list(columns[2]) == ["10\n", "10\n", "10\n"]  # thing 2
    assert list(columns[1]) == ["10.1\n", "10.1\n", "10.1\n"]  # sales value
