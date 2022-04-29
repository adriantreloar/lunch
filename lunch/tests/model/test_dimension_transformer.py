from lunch.model.dimension.dimension_structure_validator import (
    DimensionStructureValidator as validator,
)
from lunch.model.dimension.dimension_transformer import (
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
