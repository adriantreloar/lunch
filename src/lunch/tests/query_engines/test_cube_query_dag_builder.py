"""Tests for CubeQueryDagBuilder."""

from src.lunch.model.fact import Fact, FactStorage, _FactDimensionsMetadata, _FactMeasuresMetadata
from src.lunch.model.star_schema import StarSchema
from src.lunch.mvcc.version import Version
from src.lunch.queries.fully_specified_fact_query import FullySpecifiedFactQuery
from src.lunch.query_engines.cube_query_dag_builder import CubeQueryDagBuilder

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_FACT = Fact(
    name="Sales",
    fact_id=1,
    model_version=1,
    dimensions=_FactDimensionsMetadata([]),
    measures=_FactMeasuresMetadata([]),
    storage=FactStorage(index_columns=[1], data_columns=[]),
)
_STAR_SCHEMA = StarSchema(fact=_FACT, dimensions={})
_VERSION = Version(
    version=7,
    model_version=1,
    reference_data_version=1,
    cube_data_version=1,
    operations_version=1,
    website_version=1,
)


def _make_query(dimensions=None, aggregations=None) -> FullySpecifiedFactQuery:
    return FullySpecifiedFactQuery(
        star_schema=_STAR_SCHEMA,
        version=_VERSION,
        dimensions=dimensions if dimensions is not None else [],
        measures=[],
        filters=[],
        aggregations=aggregations if aggregations is not None else ["sum"],
    )


_TWO_DIMS = [
    {"dimension_id": 10, "name": "Region"},
    {"dimension_id": 20, "name": "Product"},
]
_ONE_PARTITION = {42: 7}  # partition_id=42, cube_data_version=7


# ---------------------------------------------------------------------------
# Two-dimension query tests
# ---------------------------------------------------------------------------


def test_two_dimension_query_node_count():
    """Two dims + one partition + one aggregation → 5 nodes total."""
    query = _make_query(dimensions=_TWO_DIMS, aggregations=["sum"])
    plan = CubeQueryDagBuilder.build(query=query, partitions=_ONE_PARTITION)
    # 2 FetchDimensionData + 1 FetchFactData + 1 JoinDimensionsToFact + 1 Aggregate
    assert len(plan.nodes) == 5


def test_two_dimension_query_step_names():
    """All expected step names appear in the plan."""
    query = _make_query(dimensions=_TWO_DIMS, aggregations=["sum"])
    plan = CubeQueryDagBuilder.build(query=query, partitions=_ONE_PARTITION)
    names = {bp.name for bp in plan.nodes.values()}
    assert "FetchDimensionData" in names
    assert "FetchFactData" in names
    assert "JoinDimensionsToFact" in names
    assert "Aggregate" in names


def test_two_dimension_query_edge_count():
    """Two dims + one fact → 3 edges into JoinDimensionsToFact, plus 1 edge to Aggregate."""
    query = _make_query(dimensions=_TWO_DIMS, aggregations=["sum"])
    plan = CubeQueryDagBuilder.build(query=query, partitions=_ONE_PARTITION)
    assert len(plan.edges) == 4  # 2 dim + 1 fact → join, plus join → aggregate


def test_two_dimension_query_output_uuid_in_aggregate():
    """The plan's output UUIDs are produced by Aggregate nodes."""
    query = _make_query(dimensions=_TWO_DIMS, aggregations=["sum"])
    plan = CubeQueryDagBuilder.build(query=query, partitions=_ONE_PARTITION)
    agg_nodes = {nid: bp for nid, bp in plan.nodes.items() if bp.name == "Aggregate"}
    agg_out_uuids = set()
    for bp in agg_nodes.values():
        agg_out_uuids.update(bp.outputs.keys())
    assert plan.outputs == agg_out_uuids


def test_two_dimension_query_fetch_nodes_in_edges():
    """Every fetch node appears as a 'from' node in at least one edge."""
    query = _make_query(dimensions=_TWO_DIMS, aggregations=["sum"])
    plan = CubeQueryDagBuilder.build(query=query, partitions=_ONE_PARTITION)
    fetch_node_ids = {nid for nid, bp in plan.nodes.items() if bp.name in ("FetchDimensionData", "FetchFactData")}
    from_nodes = {src for src, _ in plan.edges}
    assert fetch_node_ids.issubset(from_nodes)


def test_two_aggregations_produce_two_aggregate_nodes():
    """One Aggregate node is created per aggregation function."""
    query = _make_query(dimensions=_TWO_DIMS, aggregations=["sum", "count"])
    plan = CubeQueryDagBuilder.build(query=query, partitions=_ONE_PARTITION)
    agg_count = sum(1 for bp in plan.nodes.values() if bp.name == "Aggregate")
    assert agg_count == 2
    assert len(plan.outputs) == 2


# ---------------------------------------------------------------------------
# Zero-dimension query tests
# ---------------------------------------------------------------------------


def test_zero_dimensions_no_join_or_fetch_dimension_nodes():
    """With no dimensions, no FetchDimensionData or JoinDimensionsToFact nodes appear."""
    query = _make_query(dimensions=[], aggregations=["sum"])
    plan = CubeQueryDagBuilder.build(query=query, partitions=_ONE_PARTITION)
    names = {bp.name for bp in plan.nodes.values()}
    assert "FetchDimensionData" not in names
    assert "JoinDimensionsToFact" not in names


def test_zero_dimensions_only_fetch_fact_and_aggregate():
    """With zero dimensions only FetchFactData and Aggregate nodes are present."""
    query = _make_query(dimensions=[], aggregations=["sum"])
    plan = CubeQueryDagBuilder.build(query=query, partitions=_ONE_PARTITION)
    names = {bp.name for bp in plan.nodes.values()}
    assert names == {"FetchFactData", "Aggregate"}


def test_zero_dimensions_correct_node_count():
    """One FetchFactData + one Aggregate = two nodes."""
    query = _make_query(dimensions=[], aggregations=["sum"])
    plan = CubeQueryDagBuilder.build(query=query, partitions=_ONE_PARTITION)
    assert len(plan.nodes) == 2
