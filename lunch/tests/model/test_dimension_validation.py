import pathlib

import pytest
import yaml

from lunch.errors.validation_errors import DimensionValidationError
from lunch.model.dimension.dimension_structure_validator import (
    DimensionStructureValidator as dim_valid,
)


def test_can_contain_name_key():
    dimension = {"name": "Department"}
    assert dim_valid.validate(dimension)

@pytest.mark.parametrize(
    "comment, id_",
    [
        ("int", 1),
        ("None", None),
    ]
)
def test_can_contain_id(comment, id_):
    dimension = {"name": "Department", "id_": id_}
    assert dim_valid.validate(dimension)

@pytest.mark.parametrize(
    "comment, id_",
    [
        ("string", "foo"),
        ("dict", {1: 2}),
        ("empty_set", set()),
        ("empty_dict", {}),
        ("empty_string", ""),
    ]
)
def test_id_must_be_int(comment, id_):
    dimension = {"name": "Department", "id_": id_}
    with pytest.raises(DimensionValidationError):
        dim_valid.validate(dimension)

@pytest.mark.parametrize(
    "comment, attributes",
    [
        ("empty_list", []),
        ("list", [{"name":"foo"},{"id_":1}]),
        ("None", None),
    ]
)
def test_can_contain_attributes_key(comment, attributes):
    assert comment
    dimension = {"name": "Department", "attributes": attributes}
    assert dim_valid.validate(dimension)


@pytest.mark.parametrize(
    "comment, attributes",
    [
        ("string", "foo"),
        ("int", 1),
        ("dict", {1: 2}),
        ("empty_set", set()),
        ("empty_dict", {}),
        ("empty_string", ""),
    ]
)
def test_attributes_key_must_be_allowed_type(comment, attributes):
    assert comment
    dimension = {"name": "Department", "attributes": attributes}
    with pytest.raises(DimensionValidationError):
        dim_valid.validate(dimension)


def test_must_contain_name_key():
    dimension = {"foo": "Department"}
    with pytest.raises(DimensionValidationError):
        dim_valid.validate(dimension)


def test_department_dimension_validates():
    parent = pathlib.Path(__file__).parent
    with open(pathlib.Path(parent, "test_data", "department_dimension.yaml")) as f:
        dimension = yaml.safe_load(f)

    assert dim_valid.validate(dimension)
