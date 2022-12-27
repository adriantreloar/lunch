import asyncio
from pathlib import Path

import pandas as pd

from src.lunch.mvcc.version import Version
from src.lunch.examples.setup_managers import model_manager, version_manager
from src.lunch.import_engine.dimension_import_enactor import DimensionImportEnactor
from src.lunch.import_engine.dimension_import_optimiser import DimensionImportOptimiser
from src.lunch.import_engine.dimension_import_planner import DimensionImportPlanner
from src.lunch.managers.reference_data_manager import ReferenceDataManager
from src.lunch.storage.cache.null_dimension_data_cache import NullDimensionDataCache
from src.lunch.storage.cache.null_reference_data_cache import NullReferenceDataCache
from src.lunch.storage.dimension_data_store import DimensionDataStore
from src.lunch.storage.persistence.stringio_columnar_dimension_data_persistor import (
    StringIOColumnarDimensionDataPersistor,
)
from src.lunch.storage.persistence.stringio_reference_data_persistor import (
    StringIOReferenceDataPersistor,
)  # index etc.
from src.lunch.storage.reference_data_store import ReferenceDataStore
from src.lunch.storage.serialization.columnar_dimension_data_serializer import (
    ColumnarDimensionDataSerializer,
)
from src.lunch.storage.serialization.yaml_reference_data_serializer import (
    YamlReferenceDataSerializer,
)  # For indexes

from src.lunch.examples.save_dimension import save_dimension

async def test_dimension_data_round_trip():

    ## Create d_test
    #await save_dimension()

    data = [
        {
            "foo": "a",
            "bar": 1,
            "baz": 10,
        },
        {
            "foo": "b",
            "bar": 2,
            "baz": 20,
        },
        {
            "foo": "c",
            "bar": 3,
            "baz": 30,
        },
    ]

    df_data_d_test = pd.DataFrame(data=data)

    dimension_data_persistor = StringIOColumnarDimensionDataPersistor(
        directory=Path(
            "/home/treloarja/PycharmProjects/lunch/example_output/reference/dimension"
        )
    )
    dimension_data_cache = NullDimensionDataCache()
    dimension_serializer = ColumnarDimensionDataSerializer(
        persistor=dimension_data_persistor
    )

    # TODO - this DimensionDataStore is redundant, we should be pointing stuff at the ReferenceDataStore
    dimension_data_storage = DimensionDataStore(
        serializer=dimension_serializer, cache=dimension_data_cache
    )

    reference_data_persistor = StringIOReferenceDataPersistor(
        directory=Path(
            "/home/treloarja/PycharmProjects/lunch/example_output/reference/dimension"
        )
    )
    reference_data_cache = NullReferenceDataCache()
    reference_data_serializer = YamlReferenceDataSerializer(
        persistor=reference_data_persistor
    )

    dimension_import_planner = DimensionImportPlanner()
    dimension_import_optimiser = DimensionImportOptimiser(
        dimension_import_planner=dimension_import_planner,
        dimension_data_store=dimension_data_storage,
        model_manager=model_manager,
    )
    dimension_import_enactor = DimensionImportEnactor()

    # Dimensions and Hierarchies deserve special structures, but they also need an overall indexer
    # so that we can check whether Dimensional Data, Hierarchical Data or both have changed
    # for a given version
    reference_storage = ReferenceDataStore(
        serializer=reference_data_serializer, cache=reference_data_cache
    )

    reference_data_manager = ReferenceDataManager(
        reference_data_store=reference_storage,
        dimension_data_store=dimension_data_storage,
        dimension_import_optimiser=dimension_import_optimiser,
        dimension_import_enactor=dimension_import_enactor,
    )

    read_version = Version(version=0, model_version=0,reference_data_version=0,cube_data_version=0,operations_version=0, website_version=0)
    write_version = Version(version=1, model_version=1,reference_data_version=1,cube_data_version=0,operations_version=0, website_version=0)

    d_test = {
        "name": "Test",
        "attributes": [
            {"name": "foo"},
            {"name": "bar"},
            {"name": "baz"},
        ],
        "key": [
            "foo",
        ],
    }

    await model_manager.update_model(
        dimensions=[d_test],
        facts=[],
        read_version=read_version,
        write_version=write_version,
    )

    await reference_data_manager.update_dimension_from_dataframe(
        name="Test",
        data=df_data_d_test,
        read_version=read_version,
        write_version=write_version,
    )

    # NOTE - column 0 is id column
    columns = await dimension_data_storage.get_columns(
                                                 read_version=write_version,
                                                 dimension_id=1,
                                                 column_types={"foo": str,"bar": str,"baz": str},
                                                 filter=None)

    assert list(columns["foo"]) == ["a","b","c"]