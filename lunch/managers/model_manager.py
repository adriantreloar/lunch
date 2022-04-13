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
                 version_manager: VersionManager,
                 storage: ModelStore
                 ):
        self._storage = storage
        self._version_manager = version_manager
        self._dimension_structure_validator = dimension_structure_validator

    async def update_dimension(self, dimension: dict, last_known_version: int):
        _update_dimension(
            dimension,
            last_known_version,
            self._dimension_structure_validator,
            self._version_manager,
            self._storage
        )


async def _get_dimension(
        dimension: dict,
        version: Version,
        storage: ModelStore
        ):
    """

    :param dimension:
    :param version:
    :param storage:
    :return:
    """

    raise NotImplementedError()

async def _update_dimension(
        dimension: dict,
        last_known_version: Version,
        dimension_structure_validator: DimensionStructureValidator,
        dimension_comparer: DimensionComparer,
        version_manager: VersionManager,
        storage: ModelStore
        ):
    """

    :param dimension:
    :param last_known_version:
    :param dimension_structure_validator:
    :param dimension_comparer:
    :param version_manager:
    :param storage:
    :return:
    """

    # This could throw a validation error
    dimension_structure_validator.validate(data=dimension)

    previous_dimension= await _get_dimension(name=dimension.name, version=last_known_version, storage=storage)
    comparison = dimension_comparer.compare(dimension, previous_dimension)

    if comparison:
        with await version_manager.write_model_version(last_known_version=last_known_version) as version:

            # TODO - check references - if we have made deletions we'll need to know
            #  e.g. a deletion to an attribute
            #  we could check later (e.g. when using a DataOperation or Query)
            #  but do we really want to know we've broken
            #  something at that point?
            # Pass the comparison into the reference check, and check references at the write version
            # Referred objects will be in

            storage.put_dimension(dimension, version)

        # TODO - notify changes - here? Or elsewhere?

        storage.cache_dimension(dimension, version)

