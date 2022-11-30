from typing import Any

from numpy import dtype

from lunch.base_classes.conductor import Conductor
from lunch.import_engine.fact_import_plan import FactImportPlan
from lunch.mvcc.version import Version
from lunch.import_engine.transformers.fact_dataframe_transformer import (
    FactDataFrameTransformer,
)
from lunch.storage.fact_data_store import FactDataStore


class FactImportEnactor(Conductor):
    def __init__(self):
        pass

    async def enact_plan(
        self,
        import_plan: FactImportPlan,
        data: Any,
        read_version: Version,
        write_version: Version,
        fact_data_store: FactDataStore,
    ):
        return await _enact_plan(
            import_plan=import_plan,
            data=data,
            read_version=read_version,
            write_version=write_version,
            fact_data_store=fact_data_store,
        )


async def _enact_plan(
    import_plan: FactImportPlan,
    data: Any,
    read_version: Version,
    write_version: Version,
    fact_data_store: FactDataStore,
):

    # TODO assert data is of the type specified in the plan
    #  could be a dataframe, dask dataframe, list of open files, list of file names, file mask, and so on

    # TODO we have started with bare instructions, turn into orchestrated thing later

    # TODO NEXT - check this is working - if so, see what isn't running in the larger script

    # TODO - we have bare transformations here
    #  we need an import_plan transformer class
    #  which means we really need an ImportPlan named-dict

    column_types = {
        attribute_id: dtype(str)
        for attribute_id in (d["id_"] for d in import_plan.read_fact["attributes"])
    }

    try:
        read_columns = await fact_data_store.get_columns(
            read_version=read_version,
            fact_id=import_plan.read_fact["id_"],
            filter=import_plan.read_filter,
            column_types=column_types,
        )
    except KeyError:
        # The first time we have had data for this fact
        merged_df = data
    else:
        # We are making the
        compare_df = FactDataFrameTransformer.make_dataframe(
            columns=read_columns, dtypes=column_types
        )

        merged_df = FactDataFrameTransformer.merge(
            source_df=data, compare_df=compare_df, key=import_plan.merge_key
        )

    # dictionary of columns? attribute_id : column/iterator
    # how to represent index?
    columnar_data = FactDataFrameTransformer.columnize(data=merged_df)

    # put will have to handle its indexes too
    await fact_data_store.put(
        fact_id=import_plan.write_fact["id_"],
        columnar_data=columnar_data,
        write_version=write_version,
        read_version=read_version,
    )
