from lunch.model.dimension.dimension_transformer import DimensionTransformer
from lunch.model.fact.fact_transformer import FactTransformer
from lunch.mvcc.version import Version
from lunch.storage.cache.model_cache import ModelCache
from lunch.storage.persistence.model_persistor import ModelPersistor
from lunch.storage.serialization.model_serializer import ModelSerializer
from lunch.storage.store import Store


class ModelStore(Store):
    """Manage storage for the Model (dimensions, schemas etc.)
    Like all stores, manage persistence and cache
    """

    pass

    def __init__(
        self,
        dimension_transformer: DimensionTransformer,
        fact_transformer: FactTransformer,
        serializer: ModelSerializer,
        cache: ModelCache,
    ):
        self._serializer = serializer
        self._cache = cache
        self._dimension_transformer = dimension_transformer
        self._fact_transformer = fact_transformer

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
        return await _get_dimension(
            id_=id_, version=version, serializer=self._serializer, cache=self._cache
        )

    async def put_dimension(
        self, dimension: dict, read_version: Version, write_version: Version
    ) -> dict:

        return await _put_dimension(
            dimension=dimension,
            read_version=read_version,
            write_version=write_version,
            dimension_transformer=self._dimension_transformer,
            serializer=self._serializer,
            cache=self._cache,
        )

    async def get_fact_id(self, name: str, version: Version) -> int:
        """
        Get the integer id for a fact (since fact names can be changed)

        :param name: Name of the fact
        :param version: Read version we are querying - since names aan change
        :return: Integer id for the fact with the name
        :raises: KeyError if name is not present at this version
        """
        return await _get_fact_id(
            name=name, version=version, serializer=self._serializer, cache=self._cache
        )

    async def get_fact(self, id_: int, version: Version) -> dict:
        return await _get_fact(
            id_=id_, version=version, serializer=self._serializer, cache=self._cache
        )

    async def put_fact(
        self, fact: dict, read_version: Version, write_version: Version
    ) -> dict:

        return await _put_fact(
            fact=fact,
            read_version=read_version,
            write_version=write_version,
            fact_transformer=self._fact_transformer,
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
    dimension: dict,
    read_version: Version,
    write_version: Version,
    dimension_transformer: DimensionTransformer,
    serializer: ModelSerializer,
    cache: ModelCache,
) -> dict:

    try:
        dimension_transformer.get_id_from_dimension(dimension)
    except KeyError:
        dimension_id = await _get_dimension_id(
            name=dimension["name"],
            version=read_version,
            serializer=serializer,
            cache=cache,
        )
        out_dimension = dimension_transformer.add_id_to_dimension(
            dimension, dimension_id
        )

    out_dimension = dimension_transformer.add_model_version_to_dimension(
        out_dimension, write_version.model_version
    )

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

    await serializer.put_dimension_index(
        dimension_index=dimension_index, version=write_version
    )
    # await cache.put_dimension_index()
    return out_dimension


async def _get_fact_id(
    name: str, version: Version, serializer: ModelSerializer, cache: ModelCache
):
    try:
        return await cache.get_fact_id(name, version)
    except KeyError:
        try:
            fact_id = await serializer.get_fact_id(name, version)
        except KeyError:
            # TODO - wrap this in its own get_next_fact_id function
            try:
                fact_id = await cache.get_max_fact_id(version)
            except KeyError:
                fact_id = await serializer.get_max_fact_id(version)
            fact_id += 1
        await cache.put_fact_id(fact_id, name, version)
        return fact_id


async def _get_fact(
    id_: int, version: Version, serializer: ModelSerializer, cache: ModelCache
):
    try:
        return await cache.get_fact(id_, version)
    except KeyError:
        fact = await serializer.get_fact(id_, version)
        await cache.put_fact(fact, version)
        return fact


async def _put_fact(
    fact: dict,
    read_version: Version,
    write_version: Version,
    fact_transformer: DimensionTransformer,
    serializer: ModelSerializer,
    cache: ModelCache,
) -> dict:

    try:
        fact_transformer.get_id_from_fact(fact)
    except KeyError:
        fact_id = await _get_fact_id(
            name=fact["name"], version=read_version, serializer=serializer, cache=cache
        )
        out_fact = fact_transformer.add_id_to_fact(fact, fact_id)

    out_fact = fact_transformer.add_model_version_to_fact(
        out_fact, write_version.model_version
    )

    # Note - we cache as we put, so that later puts in a transaction can validate against cached data
    await serializer.put_fact(out_fact, write_version)
    await cache.put_fact(out_fact, write_version)

    # TODO - put these in commit write?
    try:
        fact_index = await serializer.get_fact_index(version=write_version)
    except KeyError:
        fact_index = await serializer.get_fact_index(version=read_version)
        # TODO - use a Transformer to update the index, from the fact
    fact_index[out_fact["name"]] = out_fact["id_"]

    await serializer.put_fact_index(fact_index=fact_index, version=write_version)
    # await cache.put_fact_index()
    return out_fact


async def _abort_write(
    version: Version, serializer: ModelSerializer, cache: ModelCache
):
    # Clear out half written data in the cache and in the store (in the background)
    await cache.abort_write(version)
    await serializer.abort_write(version)
