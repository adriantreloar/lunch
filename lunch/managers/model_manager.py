from lunch.managers.model_conductor import ModelConductor
from lunch.managers.version_manager import VersionManager
from lunch.base_classes.owner_conductor import OwnerConductor
from lunch.storage.model_store import ModelStore
from lunch.model.dimension.structure_validator import StructureValidator as DimensionStructureValidator

class ModelManager(OwnerConductor):

    def __init__(self,
                 storage: ModelStore,
                 version_manager: VersionManager,
                 dimension_structure_validator: DimensionStructureValidator
                 ):
        self._storage = storage
        self._version_manager = version_manager
        self._dimension_structure_validator = dimension_structure_validator

    def update_dimension(self, dimension: dict, last_known_version: int):
        ModelConductor.update_dimension(
            dimension,
            last_known_version,
            self._dimension_structure_validator,
            self._version_manager,
            self._storage
        )
