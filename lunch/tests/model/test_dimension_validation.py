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


def test_must_contain_name_key():
    dimension = {"foo": "Department"}
    with pytest.raises(DimensionValidationError):
        dim_valid.validate(dimension)


def test_department_dimension_validates():
    parent = pathlib.Path(__file__).parent
    with open(pathlib.Path(parent, "test_data", "department_dimension.yaml")) as f:
        dimension = yaml.safe_load(f)

    assert dim_valid.validate(dimension)
