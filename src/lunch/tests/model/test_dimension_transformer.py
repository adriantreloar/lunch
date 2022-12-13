from src.lunch.model.dimension.dimension_structure_validator import (
    DimensionStructureValidator as validator,
)
from src.lunch.model.dimension.dimension_transformer import (
    DimensionTransformer as transformer,
)


def test_add_attribute_ids_single_attribute():
    input_dim = {"name": "Department", "attributes": [{"name": "foo"}]}

    validator.validate(input_dim)
    output_dim = transformer.add_attribute_ids_to_dimension(input_dim)
    validator.validate(output_dim)

    assert output_dim == {
        "name": "Department",
        "attributes": [{"name": "foo", "id_": 1}],
    }


def test_add_attribute_ids_one_attribute_has_id():
    input_dim = {
        "name": "Department",
        "attributes": [{"name": "foo"}, {"name": "bar", "id_": 22}],
    }

    validator.validate(input_dim)
    output_dim = transformer.add_attribute_ids_to_dimension(input_dim)
    validator.validate(output_dim)

    assert output_dim == {
        "name": "Department",
        "attributes": [{"name": "foo", "id_": 23}, {"name": "bar", "id_": 22}],
    }


def test_add_attribute_ids_one_attribute_has_id_None_name():
    input_dim = {
        "name": "Department",
        "attributes": [{"name": "foo"}, {"name": None, "id_": 22}],
    }

    validator.validate(input_dim)
    output_dim = transformer.add_attribute_ids_to_dimension(input_dim)
    validator.validate(output_dim)

    assert output_dim == {
        "name": "Department",
        "attributes": [{"name": "foo", "id_": 23}, {"name": None, "id_": 22}],
    }


def test_add_default_storage_to_dimension_with_no_storage():

    default_storage = {"storage": "foo_storage"}

    input_dim = {
        "name": "Department",
        "attributes": [{"name": "foo"}, {"name": None}],
    }

    validator.validate(input_dim)
    output_dim = transformer.add_default_storage(
        dimension=input_dim, default_storage=default_storage
    )
    validator.validate(output_dim)

    assert output_dim == {
        "name": "Department",
        "attributes": [{"name": "foo"}, {"name": None}],
        "storage": "foo_storage",
    }

    assert input_dim == {
        "name": "Department",
        "attributes": [{"name": "foo"}, {"name": None}],
    }, "input dimension should not be mutated"


def test_add_default_storage_to_dimension_with_storage():

    default_storage = {"storage": "foo_storage"}

    input_dim = {
        "name": "Department",
        "attributes": [{"name": "foo"}, {"name": None}],
        "storage": "bar_storage",
    }

    validator.validate(input_dim)
    output_dim = transformer.add_default_storage(
        dimension=input_dim, default_storage=default_storage
    )
    validator.validate(output_dim)

    assert output_dim == {
        "name": "Department",
        "attributes": [{"name": "foo"}, {"name": None}],
        "storage": "bar_storage",
    }
