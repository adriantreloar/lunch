import pytest
import os

from lunch.model.dimensions import validator
import yaml

def test_can_contain_name_key():
    dimension = {"name": "Department"}

    validator.validate(dimension)


def test_department_dimension_validates():
    with open(os.path.join("test_data", "department_dimension.yaml")) as f:
        dimension = yaml.safe_load(f)

    validator.validate(dimension)
