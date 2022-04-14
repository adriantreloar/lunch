from lunch.managers.version_manager import VersionManager
from lunch.base_classes.conductor import Conductor
from lunch.storage.model_store import ModelStore
from lunch.model.dimension.structure_validator import StructureValidator as DimensionStructureValidator
from lunch.managers.version_manager import VersionManager
from lunch.mvcc.version import Version
from lunch.storage.model_store import ModelStore
from lunch.model.dimension.structure_validator import StructureValidator as DimensionStructureValidator
from lunch.model.dimension.comparer import Comparer as DimensionComparer


class ModelManager(Conductor):

    def __init__(self,
                 dimension_structure_validator: DimensionStructureValidator,
                 dimension_comparer: DimensionComparer,
                 version_manager: VersionManager,
                 storage: ModelStore
                 ):
        self._storage = storage
        self._version_manager = version_manager
        self._dimension_comparer=dimension_comparer
        self._dimension_structure_validator = dimension_structure_validator

    async def update_dimension(self, dimension: dict, read_version: int, write_version: int):
        return await _update_dimension(
            dimension=dimension,
            read_version=read_version,
            write_version=write_version,
            dimension_structure_validator=self._dimension_structure_validator,
            dimension_comparer=self._dimension_comparer,
            version_manager=self._version_manager,
            storage=self._storage
        )


async def _get_dimension(
        dimension: dict,
        version: int,
        version_manager: VersionManager,
        storage: ModelStore,
):
    """

    :param dimension:
    :param version:
    :param version_manager:
    :param storage:
    :return:
    """

    raise NotImplementedError()


async def _update_dimension(
        dimension: dict,
        read_version: int,
        write_version: int,
        dimension_structure_validator: DimensionStructureValidator,
        dimension_comparer: DimensionComparer,
        version_manager: VersionManager,
        storage: ModelStore
):
    """

    :param dimension:
    :param read_version:
    :param dimension_structure_validator:
    :param dimension_comparer:
    :param version_manager:
    :param storage:
    :return:
    """
    full_read_version: Version = await version_manager.get_full_version(read_version)

    # This could throw a validation error
    dimension_structure_validator.validate(data=dimension)

    previous_dimension = await _get_dimension(name=dimension.name, version=read_version,
                                              version_manager=version_manager, storage=storage)
    comparison = dimension_comparer.compare(dimension, previous_dimension)

    full_write_version = None

    # Only check and put the data if there is actually something to update
    if comparison:

        if not write_version:
            with await version_manager.write_model_version(read_version=full_read_version) as version:
                await _check_and_put(dimension, version, storage)
        else:
            full_write_version = await version_manager.get_full_version(write_version)

            await _check_and_put(dimension, full_write_version, storage)

        # TODO - notify changes - here? Or elsewhere?


async def _check_and_put(dimension: dict, version: Version, storage: ModelStore):
    # TODO - check references - if we have made deletions we'll need to know
    #  e.g. a deletion to an attribute
    #  we could check later (e.g. when using a DataOperation or Query)
    #  but do we really want to know we've broken
    #  something at that point?
    # Pass the comparison into the reference check, and check references at the write version
    # Referred objects will be in

    return await storage.put_dimension(dimension, version)

