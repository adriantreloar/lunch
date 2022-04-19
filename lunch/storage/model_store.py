from lunch.mvcc.version import Version
from lunch.storage.cache.model_cache import ModelCache
from lunch.storage.persistence.model_persistor import ModelPersistor
from lunch.storage.serialization.model_serializer import ModelSerializer
from lunch.storage.store import Store
from lunch.model.dimension.dimension_transformer import DimensionTransformer

class ModelStore(Store):
    """Manage storage for the Model (dimensions, schemas etc.)
    Like all stores, manage persistence and cache
    """

    pass

    def __init__(self, serializer: ModelSerializer, cache: ModelCache):
        self._serializer = serializer
        self._cache = cache

    async def get_dimension_id(self, name: str, version: Version) -> int:
        """
        Get the integer id for a dimension (since dimension names can be changed)

        :param name: Name of the dimension
        :param version: Read version we are querying - since names aan change
        :return: Integer id for the dimension with the name
        :raises: KeyError if name is not present at this version
        """
        return await _get_dimension_id(
            name=name, version=version, serializer=self._serializer, cache=self._cache
        )

    async def get_dimension(self, id_: int, version: Version) -> dict:
        """

        :param id_: Unique integer identifier of the dimension
        :param version:
        :return: Dimension dictionary
        """
        return await _get_dimension(
            id_=id_, version=version, serializer=self._serializer, cache=self._cache
        )

    async def put_dimension(self, dimension: dict, read_version: Version, write_version: Version) -> dict:

        return await _put_dimension(
            dimension=dimension,
            read_version=read_version,
            write_version=write_version,
            serializer=self._serializer,
            cache=self._cache,
        )

    async def abort_write(self, version: Version):
        """
        Clear out half written data in the cache and in the store for an aborted write version

        :param version: Write version that has been aborted
        :return:
        """
        await _abort_write(
            version=version, serializer=self._serializer, cache=self._cache
        )

async def _get_dimension_id(
    name: str, version: Version, serializer: ModelSerializer, cache: ModelCache
):
    try:
        return await cache.get_dimension_id(name, version)
    except KeyError:
        try:
            dimension_id = await serializer.get_dimension_id(name, version)
        except KeyError:
            # TODO - wrap this in its own get_next_dimension_id function
            try:
                dimension_id = await cache.get_max_dimension_id(version)
            except KeyError:
                dimension_id = await serializer.get_max_dimension_id(version)
            dimension_id += 1
        await cache.put_dimension_id(dimension_id, name, version)
        return dimension_id

async def _get_dimension(
    id_: int, version: Version, serializer: ModelSerializer, cache: ModelCache
):
    try:
        return await cache.get_dimension(id_, version)
    except KeyError:
        dimension = await serializer.get_dimension(id_, version)
        await cache.put_dimension(dimension, version)
        return dimension


async def _put_dimension(
    dimension: dict, read_version: Version, write_version: Version, serializer: ModelSerializer, cache: ModelCache
) -> dict:

    try:
        DimensionTransformer.get_id_from_dimension(dimension)
    except KeyError:
        dimension_id = await _get_dimension_id(name=dimension["name"], version=read_version,  serializer=serializer, cache=cache)
        out_dimension = DimensionTransformer.add_id_to_dimension(dimension, dimension_id)

    out_dimension = DimensionTransformer.add_model_version_to_dimension(out_dimension, write_version.model_version)

    # Note - we cache as we put, so that later puts in a transaction can validate against cached data
    await serializer.put_dimension(out_dimension, write_version)
    await cache.put_dimension(out_dimension, write_version)

    # TODO - put these in commit write?
    try:
        dimension_index = await serializer.get_dimension_index(version=write_version)
    except KeyError:
        dimension_index = await serializer.get_dimension_index(version=read_version)
        # TODO - use a Transformer to update the index, from the dimension
    dimension_index[out_dimension["name"]] = out_dimension["id_"]

    await serializer.put_dimension_index(dimension_index=dimension_index, version=write_version)
    # await cache.put_dimension_index()
    return out_dimension

async def _abort_write(
    version: Version, serializer: ModelSerializer, cache: ModelCache
):
    # Clear out half written data in the cache and in the store (in the background)
    await cache.abort_write(version)
    await serializer.abort_write(version)
