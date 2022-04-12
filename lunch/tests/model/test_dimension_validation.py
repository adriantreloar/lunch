import pytest
import os

from lunch.model.dimension.structure_validator import StructureValidator as dim_valid
import yaml

def test_can_contain_name_key():
    dimension = {"name": "Department"}
    assert dim_valid.validate(dimension)

def test_must_contain_name_key():
    dimension = {"foo": "Department"}
    assert not dim_valid.validate(dimension)

def test_department_dimension_validates():
    with open(os.path.join("test_data", "department_dimension.yaml")) as f:
        dimension = yaml.safe_load(f)

    assert dim_valid.validate(dimension)
