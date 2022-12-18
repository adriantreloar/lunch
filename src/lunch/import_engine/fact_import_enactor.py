from typing import Any

from numpy import dtype
import pyarrow as pa
import pyarrow.parquet as pq

from src.lunch.base_classes.conductor import Conductor
from src.lunch.plans.plan import Plan
from src.lunch.plans.basic_plan import BasicPlan
from src.lunch.mvcc.version import Version
from src.lunch.import_engine.transformers.fact_dataframe_transformer import (
    FactDataFrameTransformer,
)
from src.lunch.storage.fact_data_store import FactDataStore


class FactImportEnactor(Conductor):
    def __init__(self):
        pass

    async def enact_plan(
        self,
        append_plan: Plan,
        data: Any,
        read_version: Version,
        write_version: Version,
        fact_data_store: FactDataStore,
    ):
        return await _enact_plan(
            append_plan=append_plan,
            data=data,
            read_version=read_version,
            write_version=write_version,
            fact_data_store=fact_data_store,
        )


async def _enact_plan(
    append_plan: Plan,
    data: Any,
    read_version: Version,
    write_version: Version,
    fact_data_store: FactDataStore,
):
    if isinstance(append_plan, BasicPlan) and append_plan.name == "_import_fact_append_locally_from_dataframe":
        # TODO: if import_plan.inputs["read_filter"] is a guid, lookup the output form a previous step
        # TODO: if import_plan.inputs["merge_key"] is a guid, lookup the output form a previous step

        await _import_fact_append_locally_from_dataframe(data=data,
                                             read_version=read_version,
                                             write_version=write_version,
                                             append_plan= append_plan,
                                             fact_data_store=fact_data_store)
    else:
        raise ValueError(append_plan)

async def _import_fact_append_locally_from_dataframe(data: Any,
                                             read_version: Version,
                                             write_version: Version,
                                             append_plan: Plan,
                                             fact_data_store: FactDataStore) -> None:
    # TODO assert data is of the type specified in the plan
    #  could be a dataframe, dask dataframe, list of open files, list of file names, file mask, and so on

    # TODO we have started with bare instructions, turn into orchestrated thing later

    # TODO NEXT - check this is working - if so, see what isn't running in the larger script

    # TODO - we have bare transformations here
    #  we need an import_plan transformer class
    #  which means we really need an ImportPlan named-dict

    #return BasicPlan(
    #    name="_import_append_locally_from_dataframe",
    #    inputs={"read_fact": read_fact,
    #            "write_fact": write_fact,
    #            "read_filter": read_filter,
    #            "merge_key": merge_key,
    #            "read_fact_storage_instructions": read_fact_storage_instructions,
    #            "write_fact_storage_instructions": write_fact_storage_instructions,
    #            "data_columns": data_columns
    #            },
    #    outputs={}
    #)

    # TODO - go back to the plan creation
    #  to create the plan, we need to actually know whether we have pre-id-ed data
    #  to create the plan we need to know if we are first translating "Dimension Column" -> dimension_id
    #  thus by the time we have created the plan we should know this

    column_types = {
        attribute_id: dtype(str)
        for attribute_id in (d["id_"] for d in append_plan.inputs["read_fact"]["attributes"])
    }

    try:
        read_columns = await fact_data_store.get_columns(
            read_version=read_version,
            fact_id=append_plan.inputs["read_fact"].fact_id,
            filter=append_plan.read_filter,
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

    table = pa.Table.from_pandas(merged_df[FACT_COLUMNS])

    # put will have to handle its indexes too
    await fact_data_store.put(
        fact_id=append_plan["inputs"]["write_fact"]["id_"],
        columnar_data=columnar_data,
        write_version=write_version,
        read_version=read_version,
    )
