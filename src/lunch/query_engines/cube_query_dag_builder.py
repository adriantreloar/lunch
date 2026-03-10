from typing import Dict, Set, Tuple
from uuid import UUID, uuid4

from src.lunch.base_classes.transformer import Transformer
from src.lunch.plans.basic_plan import BasicPlan
from src.lunch.queries.fully_specified_fact_query import FullySpecifiedFactQuery
from src.lunch.plans.dag_plan import DagPlan

# partition_id -> cube_data_version
PartitionManifest = Dict[int, int]


class CubeQueryDagBuilder(Transformer):
    """Builds a DagPlan from a FullySpecifiedFactQuery and a PartitionManifest.

    Generates one FetchDimensionData node per dimension in query.dimensions,
    one FetchFactData node per entry in partitions, a JoinDimensionsToFact node
    (only when there is at least one dimension), and one Aggregate node per
    entry in query.aggregations.  All edges and UUIDs are assigned here.
    """

    @staticmethod
    def build(
        query: FullySpecifiedFactQuery,
        partitions: PartitionManifest,
    ) -> DagPlan:
        nodes: Dict[UUID, BasicPlan] = {}
        edges: Set[Tuple[UUID, UUID]] = set()

        # --- FetchDimensionData nodes ---
        dim_node_ids = []
        dim_out_uuids = []
        for dim in query.dimensions:
            node_id = uuid4()
            out_uuid = uuid4()
            nodes[node_id] = BasicPlan(
                name="FetchDimensionData",
                inputs={"dimension": dim},
                outputs={out_uuid: out_uuid},
            )
            dim_node_ids.append(node_id)
            dim_out_uuids.append(out_uuid)

        # --- FetchFactData nodes ---
        fact_node_ids = []
        fact_out_uuids = []
        for partition_id, partition_version in partitions.items():
            node_id = uuid4()
            out_uuid = uuid4()
            nodes[node_id] = BasicPlan(
                name="FetchFactData",
                inputs={"partition_id": partition_id, "version": partition_version},
                outputs={out_uuid: out_uuid},
            )
            fact_node_ids.append(node_id)
            fact_out_uuids.append(out_uuid)

        all_fetch_node_ids = dim_node_ids + fact_node_ids
        all_fetch_out_uuids = dim_out_uuids + fact_out_uuids

        # --- JoinDimensionsToFact (only when there are dimensions) ---
        if query.dimensions:
            join_node_id = uuid4()
            join_out_uuid = uuid4()
            join_inputs = {u: u for u in all_fetch_out_uuids}
            nodes[join_node_id] = BasicPlan(
                name="JoinDimensionsToFact",
                inputs=join_inputs,
                outputs={join_out_uuid: join_out_uuid},
            )
            for fetch_node_id in all_fetch_node_ids:
                edges.add((fetch_node_id, join_node_id))
            aggregate_in_uuid = join_out_uuid
            aggregate_predecessor_id = join_node_id
        else:
            # No join needed: Aggregate reads directly from the fact fetch nodes
            aggregate_in_uuid = fact_out_uuids[0] if fact_out_uuids else uuid4()
            aggregate_predecessor_id = fact_node_ids[0] if fact_node_ids else None

        # --- Aggregate nodes ---
        final_output_uuids: Set[UUID] = set()
        for aggregation in query.aggregations:
            agg_node_id = uuid4()
            agg_out_uuid = uuid4()
            nodes[agg_node_id] = BasicPlan(
                name="Aggregate",
                inputs={"aggregation": aggregation, aggregate_in_uuid: aggregate_in_uuid},
                outputs={agg_out_uuid: agg_out_uuid},
            )
            if aggregate_predecessor_id is not None:
                edges.add((aggregate_predecessor_id, agg_node_id))
            final_output_uuids.add(agg_out_uuid)

        if not final_output_uuids:
            # No aggregations: the join (or fact fetch) output is the result
            if query.dimensions:
                final_output_uuids = {join_out_uuid}
            else:
                final_output_uuids = set(fact_out_uuids)

        return DagPlan(
            nodes=nodes,
            edges=edges,
            inputs=set(),
            outputs=final_output_uuids,
        )
