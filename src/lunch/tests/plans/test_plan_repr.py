from uuid import uuid1

from src.lunch.plans.basic_plan import BasicPlan
from src.lunch.plans.dag_plan import DagPlan
from src.lunch.plans.remote_plan import RemotePlan


def test_basic_plan_repr_shows_name_and_keys():
    plan = BasicPlan(
        name="_import_locally_from_dataframe",
        inputs={"read_dimension": object(), "merge_key": [0]},
        outputs={"write_dimension": object()},
    )
    r = repr(plan)
    assert "_import_locally_from_dataframe" in r
    assert "read_dimension" in r
    assert "merge_key" in r
    assert "write_dimension" in r


def test_basic_plan_repr_does_not_include_input_values():
    sentinel = object()
    plan = BasicPlan(name="fn", inputs={"key": sentinel}, outputs={})
    assert str(sentinel) not in repr(plan)


def test_remote_plan_repr_shows_location_name_and_keys():
    plan = RemotePlan(
        location="http://host:5000",
        name="_remote_fn",
        inputs={"param_a": 1},
        outputs={"result": object()},
    )
    r = repr(plan)
    assert "http://host:5000" in r
    assert "_remote_fn" in r
    assert "param_a" in r
    assert "result" in r


def test_dag_plan_repr_contains_dag_plan_and_node_names():
    step = BasicPlan(name="step_one", inputs={}, outputs={})
    node_id = uuid1()
    plan = DagPlan(nodes={node_id: step}, edges=set(), inputs=set(), outputs=set())
    r = repr(plan)
    assert "DagPlan" in r
    assert "step_one" in r


def test_dag_plan_repr_contains_nested_basic_plan():
    inner = BasicPlan(name="inner_fn", inputs={"x": 1}, outputs={"y": 2})
    node_id = uuid1()
    plan = DagPlan(nodes={node_id: inner}, edges=set(), inputs=set(), outputs=set())
    r = repr(plan)
    assert "DagPlan" in r
    assert "BasicPlan" in r
    assert "inner_fn" in r
