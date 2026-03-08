import pytest
from mock import AsyncMock, Mock

from src.lunch.errors.validation_errors import DimensionValidationError
from src.lunch.managers.model_manager import _check_and_put_dimension
from src.lunch.model.dimension.dimension_comparer import DimensionComparer
from src.lunch.model.dimension.dimension_reference_validator import DimensionReferenceValidator
from src.lunch.mvcc.version import Version
from src.lunch.storage.model_store import ModelStore

v0 = Version(
    version=0, model_version=0, reference_data_version=0, cube_data_version=0, operations_version=0, website_version=0
)
v1 = Version(
    version=1, model_version=1, reference_data_version=0, cube_data_version=0, operations_version=0, website_version=0
)

_DIM_V1 = {"name": "Department", "id_": 1, "attributes": [{"name": "name", "id_": 1}]}
_DIM_V2_EXTRA_ATTR = {
    "name": "Department",
    "id_": 1,
    "attributes": [{"name": "name", "id_": 1}, {"name": "code", "id_": 2}],
}
_DIM_V2_REMOVED_ATTR = {"name": "Department", "id_": 1, "attributes": []}


@pytest.fixture
def storage():
    s = Mock(ModelStore)
    s.put_dimensions = AsyncMock()
    return s


async def test_first_write_no_previous_succeeds(storage):
    await _check_and_put_dimension(
        dimension=_DIM_V1,
        previous_dimension=None,
        read_version=v0,
        write_version=v1,
        storage=storage,
        dimension_comparer=DimensionComparer(),
        dimension_reference_validator=DimensionReferenceValidator(),
    )
    storage.put_dimensions.assert_called_once()


async def test_adding_attribute_succeeds(storage):
    await _check_and_put_dimension(
        dimension=_DIM_V2_EXTRA_ATTR,
        previous_dimension=_DIM_V1,
        read_version=v0,
        write_version=v1,
        storage=storage,
        dimension_comparer=DimensionComparer(),
        dimension_reference_validator=DimensionReferenceValidator(),
    )
    storage.put_dimensions.assert_called_once()


async def test_removing_attribute_raises_validation_error(storage):
    with pytest.raises(DimensionValidationError):
        await _check_and_put_dimension(
            dimension=_DIM_V2_REMOVED_ATTR,
            previous_dimension=_DIM_V1,
            read_version=v0,
            write_version=v1,
            storage=storage,
            dimension_comparer=DimensionComparer(),
            dimension_reference_validator=DimensionReferenceValidator(),
        )
    storage.put_dimensions.assert_not_called()


async def test_removing_attribute_error_prevents_storage_write(storage):
    """Validation error must be raised before put_dimensions is called."""
    with pytest.raises(DimensionValidationError):
        await _check_and_put_dimension(
            dimension={"name": "Department", "id_": 1, "attributes": []},
            previous_dimension={"name": "Department", "id_": 1, "attributes": [{"name": "name", "id_": 1}, {"name": "code", "id_": 2}]},
            read_version=v0,
            write_version=v1,
            storage=storage,
            dimension_comparer=DimensionComparer(),
            dimension_reference_validator=DimensionReferenceValidator(),
        )
    storage.put_dimensions.assert_not_called()


async def test_storage_error_propagates(storage):
    storage.put_dimensions.side_effect = IOError("disk full")
    with pytest.raises(IOError):
        await _check_and_put_dimension(
            dimension=_DIM_V2_EXTRA_ATTR,
            previous_dimension=_DIM_V1,
            read_version=v0,
            write_version=v1,
            storage=storage,
            dimension_comparer=DimensionComparer(),
            dimension_reference_validator=DimensionReferenceValidator(),
        )
