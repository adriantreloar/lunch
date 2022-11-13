from lunch.model.dimension.dimension_comparer import DimensionComparer
from lunch.model.dimension.dimension_transformer import DimensionTransformer
from lunch.model.fact.fact_comparer import FactComparer
from lunch.model.fact.fact_transformer import FactTransformer
from lunch.mvcc.version import Version
from lunch.storage.cache.model_cache import ModelCache
from lunch.storage.persistence.model_persistor import ModelPersistor
from lunch.storage.serialization.model_serializer import ModelSerializer
from lunch.storage.store import Store
from lunch.storage.transformers.dimension_index_transformer import (
    DimensionIndexTransformer,
)
from lunch.storage.transformers.fact_index_transformer import FactIndexTransformer


class ModelStore(Store):
    """Manage storage for the Model (dimensions, schemas etc.)
    Like all stores, manage persistence and cache
    """

    def __init__(
        self,
        dimension_transformer: DimensionTransformer,
        dimension_index_transformer: DimensionIndexTransformer,
        dimension_comparer: DimensionComparer,
        fact_transformer: FactTransformer,
        fact_index_transformer: FactIndexTransformer,
        fact_comparer: FactComparer,
        serializer: ModelSerializer,
        cache: ModelCache,
    ):
        self._serializer = serializer
        self._cache = cache
        self._dimension_transformer = dimension_transformer
        self._dimension_index_transformer = dimension_index_transformer
        self._dimension_comparer = dimension_comparer
        self._fact_transformer = fact_transformer
        self._fact_index_transformer = fact_index_transformer
        self._fact_comparer = fact_comparer

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

    async def put_dimensions(
        self, dimensions: list[dict], read_version: Version, write_version: Version
    ) -> None:

        return await _put_dimensions(
            dimensions=dimensions,
            read_version=read_version,
            write_version=write_version,
            dimension_transformer=self._dimension_transformer,
            dimension_index_transformer=self._dimension_index_transformer,
            dimension_comparer=self._dimension_comparer,
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

    async def put_facts(
        self, facts: list[dict], read_version: Version, write_version: Version
    ) -> dict:

        return await _put_facts(
            facts=facts,
            read_version=read_version,
            write_version=write_version,
            fact_transformer=self._fact_transformer,
            fact_index_transformer=self._fact_index_transformer,
            fact_comparer=self._fact_comparer,
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
        return await cache.get_dimension_id(name=name, version=version)
    except KeyError:
        dimension_id = await serializer.get_dimension_id(name=name, version=version)

        await cache.put_dimension_id(dimension_id=dimension_id, name=name, version=version)
        return dimension_id


async def _get_dimension(
    id_: int, version: Version, serializer: ModelSerializer, cache: ModelCache
):
    try:
        return await cache.get_dimension(id_=id_, version=version)
    except KeyError:
        dimension = await serializer.get_dimension(id_=id_, version=version)
        await cache.put_dimensions(dimensions=[dimension], version=version)
        return dimension


async def _put_dimensions(
    dimensions: list[dict],
    read_version: Version,
    write_version: Version,
    dimension_transformer: DimensionTransformer,
    dimension_index_transformer: DimensionIndexTransformer,
    dimension_comparer: DimensionComparer,
    serializer: ModelSerializer,
    cache: ModelCache,
) -> None:

    dimensions_with_ids = {}
    dimensions_without_ids = {}

    # Check which dimensions have ids or need ids
    for dimension in dimensions:
        dimension_name = dimension_transformer.get_name_from_dimension(dimension)
        try:
            # The dimension may already have the id - if it is being edited
            id_ = dimension_transformer.get_id_from_dimension(dimension)
            dimensions_with_ids[dimension_name] = dimension
        except KeyError:
            dimensions_without_ids[dimension_name] = dimension

    # Check for changes
    dimension_names_with_changes = list(dimensions_without_ids.keys())
    for dimension in dimensions_with_ids.values():
        id_ = dimension_transformer.get_id_from_dimension(dimension)

        previous_dimension = await _get_dimension(
            id_=id_, version=read_version, serializer=serializer, cache=cache
        )
        comparison = dimension_comparer.compare(dimension, previous_dimension)

        if comparison:
            dimension_name = dimension_transformer.get_name_from_dimension(dimension)
            dimension_names_with_changes.append(dimension_name)

    # Update the dimensions without ids
    max_dimension_id = await _get_max_dimension_id(
        version=read_version, serializer=serializer, cache=cache
    )
    for i, (name, dimension) in enumerate(dimensions_without_ids.items()):
        dimension_with_id = dimension_transformer.add_id_to_dimension(
            dimension, max_dimension_id + i + 1
        )
        dimensions_with_ids[name] = dimension_with_id

    dimensions_with_ids_and_versions = []
    for name, dimension in dimensions_with_ids.items():
        out_dimension = dimension_transformer.add_model_version_to_dimension(
            dimension, write_version.model_version
        )
        dimensions_with_ids_and_versions.append(out_dimension)

    dimensions_version_index_read = await _get_dimension_version_index(
        version=read_version, serializer=serializer, cache=cache
    )
    dimensions_name_index_read = await _get_dimension_name_index(
        version=read_version, serializer=serializer, cache=cache
    )

    # All the changed dimensions will be in dimensions_with_ids now
    # All of these have a version of the write-version
    dimensions_version_index_write = (
        dimension_index_transformer.update_dimension_version_index(
            index_=dimensions_version_index_read,
            write_version=write_version,
            changed_ids=[
                dimension["id_"] for dimension in dimensions_with_ids.values()
            ],
        )
    )
    dimensions_name_index_write = (
        dimension_index_transformer.update_dimension_name_index(
            index_=dimensions_name_index_read,
            changed_names_index={
                dimension["name"]: dimension["id_"]
                for dimension in dimensions_with_ids.values()
            },
        )
    )

    await _put_dimension_version_index(
        index_=dimensions_version_index_write,
        version=write_version,
        serializer=serializer,
        cache=cache,
    )
    await _put_dimension_name_index(
        index_=dimensions_name_index_write,
        version=write_version,
        serializer=serializer,
        cache=cache,
    )

    # Note - we cache as we put, so that later puts in a transaction can validate against cached data
    await serializer.put_dimensions(
        dimensions=dimensions_with_ids_and_versions, version=write_version
    )
    await cache.put_dimensions(
        dimensions=dimensions_with_ids_and_versions, version=write_version
    )


async def _get_max_dimension_id(
    version: Version, serializer: ModelSerializer, cache: ModelCache
) -> int:
    if not version.version:
        return 0

    try:
        return await cache.get_max_dimension_id(version=version)
    except KeyError:
        return await serializer.get_max_dimension_id(version=version)


async def _get_dimension_name_index(
    version: Version, serializer: ModelSerializer, cache: ModelCache
) -> dict[str, int]:
    if not version.version:
        return {}

    try:
        return await cache.get_dimension_name_index(version=version)
    except KeyError:
        return await serializer.get_dimension_name_index(version=version)


async def _get_dimension_version_index(
    version: Version, serializer: ModelSerializer, cache: ModelCache
) -> dict[int, int]:
    if not version.version:
        return {}

    try:
        return await cache.get_dimension_version_index(version=version)
    except KeyError:
        return await serializer.get_dimension_version_index(version=version)


async def _put_dimension_name_index(
    index_: dict[str, int],
    version: Version,
    serializer: ModelSerializer,
    cache: ModelCache,
):
    await serializer.put_dimension_name_index(index_=index_, version=version)
    await cache.put_dimension_name_index(index_=index_, version=version)


async def _put_dimension_version_index(
    index_: dict[int, int],
    version: Version,
    serializer: ModelSerializer,
    cache: ModelCache,
):
    await serializer.put_dimension_version_index(index_=index_, version=version)
    await cache.put_dimension_version_index(index_=index_, version=version)


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
        await cache.put_facts([fact], version)
        return fact


async def _get_max_fact_id(
    version: Version, serializer: ModelSerializer, cache: ModelCache
) -> int:
    if not version.version:
        return 0

    try:
        return await cache.get_max_fact_id(version=version)
    except KeyError:
        return await serializer.get_max_fact_id(version=version)


async def _get_fact_name_index(
    version: Version, serializer: ModelSerializer, cache: ModelCache
) -> dict[str, int]:
    if not version.version:
        return {}

    try:
        return await cache.get_fact_name_index(version=version)
    except KeyError:
        return await serializer.get_fact_name_index(version=version)


async def _get_fact_version_index(
    version: Version, serializer: ModelSerializer, cache: ModelCache
) -> dict[int, int]:
    if not version.version:
        return {}

    try:
        return await cache.get_fact_version_index(version=version)
    except KeyError:
        return await serializer.get_fact_version_index(version=version)


async def _put_fact_name_index(
    index_: dict[str, int],
    version: Version,
    serializer: ModelSerializer,
    cache: ModelCache,
):
    await serializer.put_fact_name_index(fact_index=index_, version=version)
    await cache.put_fact_name_index(index_=index_, version=version)


async def _put_fact_version_index(
    index_: dict[int, int],
    version: Version,
    serializer: ModelSerializer,
    cache: ModelCache,
):
    await serializer.put_fact_version_index(fact_index=index_, version=version)
    await cache.put_fact_version_index(index_=index_, version=version)


async def _put_facts(
    facts: list[dict],
    read_version: Version,
    write_version: Version,
    fact_comparer: FactComparer,
    fact_transformer: FactTransformer,
    fact_index_transformer: FactIndexTransformer,
    serializer: ModelSerializer,
    cache: ModelCache,
) -> dict:

    # TODO - the fact checking code is almost identical to the dimension checking code, refactor
    facts_with_ids = {}
    facts_without_ids = {}

    for fact in facts:
        fact_name = fact_transformer.get_name_from_fact(fact)
        try:
            # The fact may already have the id - if it is being edited
            id_ = fact_transformer.get_id_from_fact(fact)
            facts_with_ids[fact_name] = fact
        except KeyError:
            facts_without_ids[fact_name] = fact

    # Check for changes
    fact_names_with_changes = list(facts_without_ids.keys())
    for fact in facts_with_ids.values():
        id_ = fact_transformer.get_id_from_fact(fact)

        previous_fact = await _get_fact(
            id_=id_, version=read_version, serializer=serializer, cache=cache
        )
        comparison = fact_comparer.compare(fact, previous_fact)

        if comparison:
            fact_name = fact_transformer.get_name_from_fact(fact)
            fact_names_with_changes.append(fact_name)

    # Update the facts without ids
    max_fact_id = await _get_max_fact_id(
        version=read_version, serializer=serializer, cache=cache
    )
    for i, (name, dimension) in enumerate(facts_without_ids.items()):
        fact_with_id = fact_transformer.add_id_to_fact(dimension, max_fact_id + i + 1)
        facts_with_ids[name] = fact_with_id

    facts_with_ids_and_versions = []
    for name, fact in facts_with_ids.items():
        out_fact = fact_transformer.add_model_version_to_fact(
            fact, write_version.model_version
        )
        facts_with_ids_and_versions.append(out_fact)

    facts_version_index_read = await _get_fact_version_index(
        version=read_version, serializer=serializer, cache=cache
    )
    facts_name_index_read = await _get_fact_name_index(
        version=read_version, serializer=serializer, cache=cache
    )

    # All the changed facts will be in facts_with_ids now
    # All of these have a version of the write-version
    facts_version_index_write = fact_index_transformer.update_fact_version_index(
        index_=facts_version_index_read,
        write_version=write_version,
        changed_ids=[fact["id_"] for fact in facts_with_ids.values()],
    )
    facts_name_index_write = fact_index_transformer.update_fact_name_index(
        index_=facts_name_index_read,
        changed_names_index={
            fact["name"]: fact["id_"] for fact in facts_with_ids.values()
        },
    )

    await _put_fact_version_index(
        index_=facts_version_index_write,
        version=write_version,
        serializer=serializer,
        cache=cache,
    )
    await _put_fact_name_index(
        index_=facts_name_index_write,
        version=write_version,
        serializer=serializer,
        cache=cache,
    )

    # Note - we cache as we put, so that later puts in a transaction can validate against cached data
    await serializer.put_facts(facts_with_ids_and_versions, write_version)
    await cache.put_facts(facts_with_ids_and_versions, write_version)

    # TODO 20221030, I don't know what I was intending to return
    return {}


async def _abort_write(
    version: Version, serializer: ModelSerializer, cache: ModelCache
):
    # Clear out half written data in the cache and in the store (in the background)
    await cache.abort_write(version)
    await serializer.abort_write(version)
