from uuid import uuid4

from src.lunch.base_classes.data import Data
from src.lunch.model.fact import (
    Fact,
    FactStorage,
    _FactDimensionsMetadata,
    _FactMeasuresMetadata,
)
from src.lunch.model.star_schema import StarSchema
from src.lunch.mvcc.version import Version
from src.lunch.plans.basic_plan import BasicPlan
from src.lunch.queries.fully_specified_fact_query import FullySpecifiedFactQuery
from src.lunch.query_engines.dag_plan import DagPlan
from src.lunch.query_engines.query_result import QueryResult

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
    version=1,
    model_version=1,
    reference_data_version=1,
    cube_data_version=1,
    operations_version=1,
    website_version=1,
)
_QUERY = FullySpecifiedFactQuery(
    star_schema=_STAR_SCHEMA,
    version=_VERSION,
    dimensions=[],
    measures=[],
    filters=[],
    aggregations=[],
)


def _make_basic_plan(name: str) -> BasicPlan:
    return BasicPlan(name=name, inputs={}, outputs={})


# ---------------------------------------------------------------------------
# DagPlan tests
# ---------------------------------------------------------------------------


def test_dag_plan_stores_fields():
    node_id = uuid4()
    node = _make_basic_plan("FetchData")
    plan = DagPlan(
        nodes={node_id: node},
        edges=set(),
        inputs=set(),
        outputs={node_id},
    )
    assert plan.nodes == {node_id: node}
    assert plan.edges == set()
    assert plan.inputs == set()
    assert plan.outputs == {node_id}


def test_dag_plan_is_data_instance():
    plan = DagPlan(nodes={}, edges=set(), inputs=set(), outputs=set())
    assert isinstance(plan, Data)


def test_dag_plan_no_edges_is_fully_parallel():
    """A DagPlan with multiple nodes and no edges represents a fully-parallel structure."""
    id_a, id_b, id_c = uuid4(), uuid4(), uuid4()
    plan = DagPlan(
        nodes={
            id_a: _make_basic_plan("A"),
            id_b: _make_basic_plan("B"),
            id_c: _make_basic_plan("C"),
        },
        edges=set(),
        inputs=set(),
        outputs={id_a, id_b, id_c},
    )
    assert len(plan.nodes) == 3
    assert plan.edges == set()


def test_dag_plan_chain_expresses_serial_dependency():
    """A chain A→B→C correctly expresses serial dependency via edges."""
    id_a, id_b, id_c = uuid4(), uuid4(), uuid4()
    plan = DagPlan(
        nodes={
            id_a: _make_basic_plan("A"),
            id_b: _make_basic_plan("B"),
            id_c: _make_basic_plan("C"),
        },
        edges={(id_a, id_b), (id_b, id_c)},
        inputs=set(),
        outputs={id_c},
    )
    assert (id_a, id_b) in plan.edges
    assert (id_b, id_c) in plan.edges
    assert (id_a, id_c) not in plan.edges


# ---------------------------------------------------------------------------
# QueryResult tests
# ---------------------------------------------------------------------------


def test_query_result_stores_fields():
    import pandas as pd

    node_id = uuid4()
    dag = DagPlan(
        nodes={node_id: _make_basic_plan("step")},
        edges=set(),
        inputs=set(),
        outputs={node_id},
    )
    df = pd.DataFrame({"x": [1, 2, 3]})
    result = QueryResult(data=df, query=_QUERY, plan=dag)
    assert result.data is df
    assert result.query is _QUERY
    assert result.plan is dag


def test_query_result_is_data_instance():
    dag = DagPlan(nodes={}, edges=set(), inputs=set(), outputs=set())
    result = QueryResult(data=None, query=_QUERY, plan=dag)
    assert isinstance(result, Data)
