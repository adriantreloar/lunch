from src.lunch.plans.basic_plan import BasicPlan
from src.lunch.plans.parallel_plan import ParallelPlan
from src.lunch.plans.remote_plan import RemotePlan
from src.lunch.plans.serial_plan import SerialPlan


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


def test_serial_plan_repr_contains_step_reprs():
    step = BasicPlan(name="step_one", inputs={}, outputs={})
    plan = SerialPlan(steps=[step])
    r = repr(plan)
    assert "SerialPlan" in r
    assert "step_one" in r


def test_parallel_plan_repr_contains_step_reprs():
    step = BasicPlan(name="step_two", inputs={}, outputs={})
    plan = ParallelPlan(steps=[step])
    r = repr(plan)
    assert "ParallelPlan" in r
    assert "step_two" in r


def test_nested_plan_repr_is_readable():
    inner = BasicPlan(name="inner_fn", inputs={"x": 1}, outputs={"y": 2})
    outer = SerialPlan(steps=[inner])
    r = repr(outer)
    assert "SerialPlan" in r
    assert "BasicPlan" in r
    assert "inner_fn" in r
