import pytest
from mock import AsyncMock, Mock, call

from src.lunch.errors.validation_errors import DimensionValidationError
from src.lunch.globals.global_state import GlobalState
from src.lunch.managers.model_manager import ModelManager
from src.lunch.model.dimension.dimension_comparer import DimensionComparer
from src.lunch.model.dimension.dimension_reference_validator import DimensionReferenceValidator
from src.lunch.model.dimension.dimension_structure_validator import DimensionStructureValidator
from src.lunch.model.dimension.dimension_transformer import DimensionTransformer
from src.lunch.model.fact import (
    Fact,
    FactDimensionMetadatum,
    FactTransformer,
    _FactDimensionsMetadata,
    _FactMeasuresMetadata,
)
from src.lunch.model.old_fact.fact_comparer import FactComparer
from src.lunch.model.old_fact.fact_reference_validator import FactReferenceValidator
from src.lunch.model.star_schema import StarSchema
from src.lunch.mvcc.version import Version
from src.lunch.storage.model_store import ModelStore

v0 = Version(version=0, model_version=0, reference_data_version=0,
             cube_data_version=0, operations_version=0, website_version=0)
v1 = Version(version=1, model_version=1, reference_data_version=0,
             cube_data_version=0, operations_version=0, website_version=0)

_DIM = {"name": "Department", "attributes": [{"name": "name", "id_": 1}], "id_": 1}


def _fact_referencing_dim_by_name(dim_name: str) -> Fact:
    dims = _FactDimensionsMetadata([
        FactDimensionMetadatum(
            name="dim_fk", view_order=1, column_id=1, dimension_id=0,
            dimension_name=dim_name,
        )
    ])
    return Fact(name="Sales", dimensions=dims, measures=_FactMeasuresMetadata([]))


def _fact_referencing_dim_by_id(dim_id: int) -> Fact:
    dims = _FactDimensionsMetadata([
        FactDimensionMetadatum(
            name="dim_fk", view_order=1, column_id=1, dimension_id=dim_id,
        )
    ])
    return Fact(name="Sales", dimensions=dims, measures=_FactMeasuresMetadata([]))


@pytest.fixture
def manager_and_mocks():
    storage = Mock(ModelStore)
    storage.put_dimensions = AsyncMock()
    storage.put_facts = AsyncMock()
    storage.get_dimension_id = AsyncMock()
    storage.get_dimension = AsyncMock()
    storage.get_fact_id = AsyncMock()
    storage.get_fact = AsyncMock()

    validator = Mock(DimensionStructureValidator)
    comparer = Mock(DimensionComparer)
    ref_validator = Mock(DimensionReferenceValidator)
    transformer = Mock(DimensionTransformer)
    fact_comparer = FactComparer()
    fact_ref_validator = FactReferenceValidator()

    manager = ModelManager(
        dimension_structure_validator=validator,
        dimension_comparer=comparer,
        dimension_reference_validator=ref_validator,
        dimension_transformer=transformer,
        fact_comparer=fact_comparer,
        fact_reference_validator=fact_ref_validator,
        storage=storage,
        global_state=GlobalState(),
    )
    return manager, storage, validator, transformer


# ---------------------------------------------------------------------------
# update_model — dimension validation failure
# ---------------------------------------------------------------------------

async def test_update_model_validation_error_prevents_dimension_write(manager_and_mocks):
    manager, storage, validator, _ = manager_and_mocks
    validator.validate.side_effect = DimensionValidationError("bad dimension")

    with pytest.raises(DimensionValidationError):
        await manager.update_model(
            dimensions=[{"name": "Bad"}], facts=[],
            read_version=v0, write_version=v1,
        )

    storage.put_dimensions.assert_not_called()
    storage.put_facts.assert_not_called()


async def test_update_model_validation_error_on_second_dimension_prevents_write(manager_and_mocks):
    manager, storage, validator, transformer = manager_and_mocks
    transformer.add_attribute_ids_to_dimension.side_effect = lambda dimension: dimension
    # First call passes, second (post-transform) call fails
    validator.validate.side_effect = [None, DimensionValidationError("bad after transform")]

    with pytest.raises(DimensionValidationError):
        await manager.update_model(
            dimensions=[{"name": "Dept"}], facts=[],
            read_version=v0, write_version=v1,
        )

    storage.put_dimensions.assert_not_called()


# ---------------------------------------------------------------------------
# update_model — storage failure
# ---------------------------------------------------------------------------

async def test_update_model_put_dimensions_error_propagates(manager_and_mocks):
    manager, storage, validator, transformer = manager_and_mocks
    validator.validate.return_value = None
    transformer.add_attribute_ids_to_dimension.side_effect = lambda dimension: dimension
    storage.put_dimensions.side_effect = IOError("disk full")

    with pytest.raises(IOError):
        await manager.update_model(
            dimensions=[{"name": "Dept"}], facts=[],
            read_version=v0, write_version=v1,
        )


async def test_update_model_put_facts_error_propagates(manager_and_mocks):
    manager, storage, validator, transformer = manager_and_mocks
    validator.validate.return_value = None
    transformer.add_attribute_ids_to_dimension.side_effect = lambda dimension: dimension
    storage.put_dimensions.return_value = None
    storage.get_dimension_id.return_value = 1
    storage.get_dimension.return_value = _DIM
    transformer.add_default_storage.return_value = _DIM
    storage.put_facts.side_effect = IOError("disk full")

    with pytest.raises(IOError):
        await manager.update_model(
            dimensions=[], facts=[_fact_referencing_dim_by_name("Department")],
            read_version=v0, write_version=v1,
        )


# ---------------------------------------------------------------------------
# update_model — unknown dimension referenced by a fact
# ---------------------------------------------------------------------------

async def test_update_model_fact_references_unknown_dimension_raises_key_error(manager_and_mocks):
    manager, storage, validator, transformer = manager_and_mocks
    validator.validate.return_value = None
    storage.put_dimensions.return_value = None
    storage.get_dimension_id.side_effect = KeyError("NoSuchDim")

    with pytest.raises(KeyError):
        await manager.update_model(
            dimensions=[],
            facts=[_fact_referencing_dim_by_name("NoSuchDim")],
            read_version=v0, write_version=v1,
        )

    storage.put_facts.assert_not_called()


# ---------------------------------------------------------------------------
# get_dimension_by_name — not found
# ---------------------------------------------------------------------------

async def test_get_dimension_by_name_not_found_raises_key_error(manager_and_mocks):
    manager, storage, _, _ = manager_and_mocks
    storage.get_dimension_id.side_effect = KeyError("Department")

    with pytest.raises(KeyError):
        await manager.get_dimension_by_name(
            name="Department", version=v1, add_default_storage=False
        )


async def test_get_dimension_by_name_id_found_but_data_missing_raises_key_error(manager_and_mocks):
    manager, storage, _, _ = manager_and_mocks
    storage.get_dimension_id.return_value = 1
    storage.get_dimension.side_effect = KeyError(1)

    with pytest.raises(KeyError):
        await manager.get_dimension_by_name(
            name="Department", version=v1, add_default_storage=False
        )


# ---------------------------------------------------------------------------
# get_star_schema_model_by_fact_name — happy path
# ---------------------------------------------------------------------------

async def test_get_star_schema_returns_star_schema(manager_and_mocks):
    manager, storage, _, _ = manager_and_mocks
    fact = _fact_referencing_dim_by_id(42)
    _dim_42 = {"name": "Department", "attributes": [{"name": "name", "id_": 1}], "id_": 42}
    storage.get_fact_id.return_value = 1
    storage.get_fact.return_value = fact
    storage.get_dimension.return_value = _dim_42

    result = await manager.get_star_schema_model_by_fact_name(name="Sales", version=v1)

    assert isinstance(result, StarSchema)
    assert result.fact == fact
    assert result.dimensions[42] == _dim_42


# ---------------------------------------------------------------------------
# get_star_schema_model_by_fact_name — missing referenced dimension
# ---------------------------------------------------------------------------

async def test_get_star_schema_missing_fact_raises_key_error(manager_and_mocks):
    manager, storage, _, _ = manager_and_mocks
    storage.get_fact_id.side_effect = KeyError("Sales")

    with pytest.raises(KeyError):
        await manager.get_star_schema_model_by_fact_name(name="Sales", version=v1)


async def test_get_star_schema_fact_found_but_dimension_missing_raises_key_error(manager_and_mocks):
    manager, storage, _, _ = manager_and_mocks
    storage.get_fact_id.return_value = 1
    storage.get_fact.return_value = _fact_referencing_dim_by_id(42)
    storage.get_dimension.side_effect = KeyError(42)

    with pytest.raises(KeyError):
        await manager.get_star_schema_model_by_fact_name(name="Sales", version=v1)


# ---------------------------------------------------------------------------
# update_model — dimension resolved by id (not by name lookup)
# ---------------------------------------------------------------------------

async def test_update_model_resolves_fact_dimension_by_id(manager_and_mocks):
    manager, storage, validator, transformer = manager_and_mocks
    validator.validate.return_value = None
    transformer.add_attribute_ids_to_dimension.side_effect = lambda dimension: dimension
    storage.put_dimensions.return_value = None
    _dim_42 = {"name": "Department", "attributes": [{"name": "name", "id_": 1}], "id_": 42}
    storage.get_dimension.return_value = _dim_42
    storage.put_facts.return_value = None

    await manager.update_model(
        dimensions=[],
        facts=[_fact_referencing_dim_by_id(42)],
        read_version=v0,
        write_version=v1,
    )

    # dimension resolved directly by id — get_dimension_id must NOT be called
    storage.get_dimension_id.assert_not_called()
    storage.get_dimension.assert_called_once()
    storage.put_facts.assert_called_once()
