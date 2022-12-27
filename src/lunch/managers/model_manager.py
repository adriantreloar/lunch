from src.lunch.base_classes.conductor import Conductor
from src.lunch.globals.global_state import GlobalState
from src.lunch.model.dimension.dimension_comparer import DimensionComparer
from src.lunch.model.dimension.dimension_reference_validator import (
    DimensionReferenceValidator,
)
from src.lunch.model.dimension.dimension_structure_validator import (
    DimensionStructureValidator,
)
from src.lunch.model.dimension.dimension_transformer import DimensionTransformer
from src.lunch.model.fact import Fact, FactTransformer
from src.lunch.model.star_schema import StarSchema, StarSchemaTransformer
from src.lunch.mvcc.version import Version
from src.lunch.storage.model_store import ModelStore


class ModelManager(Conductor):
    def __init__(
        self,
        dimension_structure_validator: DimensionStructureValidator,
        dimension_comparer: DimensionComparer,
        dimension_reference_validator: DimensionReferenceValidator,
        dimension_transformer: DimensionTransformer,
        storage: ModelStore,
        global_state: GlobalState,
    ):
        self._storage = storage
        self._dimension_structure_validator = dimension_structure_validator
        self._dimension_comparer = dimension_comparer
        self._dimension_reference_validator = dimension_reference_validator
        self._dimension_transformer = dimension_transformer

        self._global_state = global_state

    async def update_model(
        self,
        dimensions: list[dict],
        facts: list[Fact],
        read_version: Version,
        write_version: Version,
    ):
        return await _update_model(
            dimensions=dimensions,
            facts=facts,
            read_version=read_version,
            write_version=write_version,
            dimension_structure_validator=self._dimension_structure_validator,
            dimension_comparer=self._dimension_comparer,
            dimension_transformer=self._dimension_transformer,
            storage=self._storage,
        )

    async def get_dimension_by_name(
        self, name: str, version: Version, add_default_storage: bool
    ) -> dict:
        return await _get_dimension_by_name(
            name=name,
            version=version,
            add_default_storage=add_default_storage,
            storage=self._storage,
            default_storage=self._global_state.default_dimension_storage,
            dimension_transformer=self._dimension_transformer,
        )

    async def get_dimension(
        self,
        id_: int,
        version: Version,
    ) -> dict:
        return await _get_dimension(id_=id_, version=version, storage=self._storage)


    async def get_fact_by_name(
        self, name: str, version: Version
    ) -> Fact:
        return await _get_fact_by_name(
            name=name,
            version=version,
            storage=self._storage,
        )

    async def get_star_schema_model_by_fact_name(
        self, name: str, version: Version
    ) -> StarSchema:
        fact = await _get_fact_by_name(name=name, version=version, storage=self._storage)
        dimensions_by_id = {}
        for dimension_id in FactTransformer.get_unique_dimension_ids(fact):
            dimensions_by_id[dimension_id] = await _get_dimension(id_=dimension_id, version=version, storage=self._storage)

        return StarSchema(fact=fact, dimensions=dimensions_by_id)

async def _get_dimension_id(
    name: str,
    version: Version,
    storage: ModelStore,
) -> int:
    return await storage.get_dimension_id(name=name, version=version)


async def _get_dimension(
    id_: int,
    version: Version,
    storage: ModelStore,
) -> dict:
    return await storage.get_dimension(id_=id_, version=version)


async def _get_dimension_by_name(
    name: str,
    version: Version,
    add_default_storage: bool,
    default_storage: dict,
    dimension_transformer: DimensionTransformer,
    storage: ModelStore,
) -> dict:
    id_ = await _get_dimension_id(name=name, version=version, storage=storage)
    dim = await _get_dimension(id_=id_, version=version, storage=storage)

    if add_default_storage:
        dim = dimension_transformer.add_default_storage(
            dimension=dim, default_storage=default_storage
        )

    return dim


async def _get_fact_by_name(
    name: str,
    version: Version,
    storage: ModelStore,
) -> Fact:
    id_ = await _get_fact_id(name=name, version=version, storage=storage)
    return await _get_fact(id_=id_, version=version, storage=storage)

async def _get_fact_id(
    name: str,
    version: Version,
    storage: ModelStore,
) -> int:
    return await storage.get_fact_id(name=name, version=version)


async def _get_fact(
    id_: int,
    version: Version,
    storage: ModelStore,
) -> Fact:
    return await storage.get_fact(id_=id_, version=version)


async def _update_model(
    dimensions: list[dict],
    facts: list[Fact],
    read_version: Version,
    write_version: Version,
    dimension_structure_validator: DimensionStructureValidator,
    dimension_comparer: DimensionComparer,
    dimension_transformer: DimensionTransformer,
    storage: ModelStore,
):

    # This could throw a validation error
    out_dimensions = []
    for dimension in dimensions:
        dimension_structure_validator.validate(data=dimension)
        dimension = dimension_transformer.add_attribute_ids_to_dimension(
            dimension=dimension
        )
        dimension_structure_validator.validate(data=dimension)
        out_dimensions.append(dimension)

    await storage.put_dimensions(
        read_version=read_version, write_version=write_version, dimensions=out_dimensions
    )

    # TODO - it would make sense to do basic validations and change detection BEFORE creating the new write version,
    #  Thus we would only have model_version flagged in the write version if we definitely needed one
    #  However, currently checks aren't done before write_version creation, so we will need to create indexes
    #  for dimensions and facts everytime


    # TODO - package this into a function
    out_facts = []
    for fact in facts:
        # add default view order if there is none
        fact = FactTransformer.fill_default_view_order(fact=fact)
        # add column ids if we don't have them
        fact = FactTransformer.fill_default_column_ids(fact=fact)

        # TODO - package this into a function
        dimensions = []
        # Gather canonical versions of all of the dimensions used in the fact
        # validate fact cross references (and fill in dimension ids) against WRITE version of dimension
        for dimension_metadata in fact.dimensions:
            dimension_id = dimension_metadata.dimension_id
            dimension: dict = None
            if dimension_id != 0:
                dimension = await _get_dimension(id_=dimension_id, version=write_version, storage=storage)
            if dimension is None:
                dimension_name = dimension_metadata.dimension_name
                dimension = await _get_dimension_by_name(name=dimension_name,
                                                         version=write_version,
                                                         storage=storage,
                                                         add_default_storage=False,
                                                         default_storage=None,
                                                         dimension_transformer=dimension_transformer)
            dimensions.append(dimension)

        fact = FactTransformer.fill_dimension_info(fact=fact, dimensions=dimensions)

        out_facts.append(fact)

    await storage.put_facts(
        read_version=read_version, write_version=write_version, facts=out_facts
    )


async def _update_fact(
    fact: dict,
    read_version: Version,
    write_version: Version,
    storage: ModelStore,
):

    # This could throw a validation error
    fact_structure_validator.validate(data=fact)

    try:
        # The fact may already have the id - if it is being edited
        fact_transformer.get_id_from_fact(fact)
    except KeyError:
        fact_name = fact_transformer.get_name_from_fact(fact)

        try:
            fact_id = await _get_fact_id(
                name=fact_name, version=read_version, storage=storage
            )

            previous_fact = await _get_fact(
                id_=fact_id, version=read_version, storage=storage
            )
        except KeyError:
            # There was no previous fact
            previous_fact = {}

    comparison = fact_comparer.compare(fact, previous_fact)

    # Only check and put the data if there is actually something to update
    if comparison:
        await _check_and_put_fact(
            fact=fact,
            read_version=read_version,
            write_version=write_version,
            storage=storage,
        )

        # TODO - notify changes - here? Or elsewhere?


async def _check_and_put_dimension(
    dimension: dict, read_version: Version, write_version: Version, storage: ModelStore
):
    # TODO - check references - if we have made deletions we'll need to know
    #  e.g. a deletion to an attribute
    #  we could check later (e.g. when using a DataOperation or Query)
    #  but do we really want to know we've broken
    #  something at that point?
    # Pass the comparison into the reference check, and check references at the write version
    # Referred objects will be in

    return await storage.put_dimensions(
        [dimension], read_version=read_version, write_version=write_version
    )


async def _check_and_put_fact(
    fact: Fact, read_version: Version, write_version: Version, storage: ModelStore
):
    # TODO - check references - if we have made deletions we'll need to know
    # Pass the comparison into the reference check, and check references at the write version
    # Referred objects will be in

    return await storage.put_facts(
        [fact], read_version=read_version, write_version=write_version
    )
