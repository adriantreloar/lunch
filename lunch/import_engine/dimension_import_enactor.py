from typing import Any

from lunch.base_classes.conductor import Conductor
from lunch.mvcc.version import Version
from lunch.reference_data.transformers.dimension_dataframe_transformer import (
    DimensionDataFrameTransformer,
)
from lunch.storage.dimension_data_store import DimensionDataStore


class DimensionImportEnactor(Conductor):
    def __init__(self):
        pass

    async def enact_plan(
        self,
        import_plan: dict,
        data: Any,
        read_version: Version,
        write_version: Version,
        dimension_data_store: DimensionDataStore,
    ):
        return await _enact_plan(
            import_plan=import_plan,
            data=data,
            read_version=read_version,
            write_version=write_version,
            dimension_data_store=dimension_data_store,
        )


async def _enact_plan(
    self,
    import_plan: dict,
    data: Any,
    read_version: Version,
    write_version: Version,
    dimension_data_store: DimensionDataStore,
):

    # TODO assert data is of the type specified in the plan
    #  could be a dataframe, dask dataframe, list of open files, list of file names, file mask, and so on

    # TODO we have started with bare instructions, turn into orchestrated thing later

    # TODO NEXT - check this is working - if so, see what isn't running in the larger script
    read_columns, read_index = await dimension_data_store.get(
        read_version=read_version,
        dimension_id=import_plan["dimension_id"],
        filter=import_plan.get("read_filter"),
    )

    # We are making the
    compare_df = await DimensionDataFrameTransformer.make_dataframe(
        columns=read_columns, index=read_index
    )

    merged_df = await DimensionDataFrameTransformer.merge(
        source_df=data, compare_df=compare_df, key=import_plan["merge_key"]
    )

    # dictionary of columns? attribute_id : column/iterator
    # how to represent index?
    columnar_data = DimensionDataFrameTransformer.columnize(data=merged_df)

    # put will have to handle its indexes too
    dimension_data_store.put(columnar_data, write_version)
