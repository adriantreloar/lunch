from lunch.base_classes.conductor import Conductor
from lunch.managers.version_manager import VersionManager
from lunch.model.dimension.dimension_comparer import DimensionComparer
from lunch.model.dimension.dimension_reference_validator import (
    DimensionReferenceValidator,
)
from lunch.model.dimension.dimension_structure_validator import (
    DimensionStructureValidator,
)
from lunch.model.dimension.dimension_transformer import DimensionTransformer
from lunch.model.fact.fact_comparer import FactComparer
from lunch.model.fact.fact_reference_validator import FactReferenceValidator
from lunch.model.fact.fact_structure_validator import FactStructureValidator
from lunch.model.fact.fact_transformer import FactTransformer
from lunch.mvcc.version import Version
from lunch.storage.model_store import ModelStore


class ModelManager(Conductor):
    def __init__(
        self,
        dimension_structure_validator: DimensionStructureValidator,
        dimension_comparer: DimensionComparer,
        dimension_reference_validator: DimensionReferenceValidator,
        dimension_transformer: DimensionTransformer,
        fact_structure_validator: FactStructureValidator,
        fact_comparer: FactComparer,
        fact_reference_validator: FactReferenceValidator,
        fact_transformer: FactTransformer,
        version_manager: VersionManager,
        storage: ModelStore,
    ):
        self._storage = storage
        self._version_manager = version_manager
        self._dimension_structure_validator = dimension_structure_validator
        self._dimension_comparer = dimension_comparer
        self._dimension_reference_validator = dimension_reference_validator
        self._dimension_transformer = dimension_transformer
        self._fact_structure_validator = fact_structure_validator
        self._fact_comparer = fact_comparer
        self._fact_reference_validator = fact_reference_validator
        self._fact_transformer = fact_transformer

    async def update_model(
        self, dimensions: list[dict], facts: list[dict], read_version: int, write_version: int
    ):
        return await _update_model(
            dimensions=dimensions,
            facts=facts,
            read_version=read_version,
            write_version=write_version,
            dimension_structure_validator=self._dimension_structure_validator,
            dimension_comparer=self._dimension_comparer,
            dimension_transformer=self._dimension_transformer,
            fact_structure_validator=self._fact_structure_validator,
            fact_comparer=self._fact_comparer,
            fact_transformer=self._fact_transformer,
            version_manager=self._version_manager,
            storage=self._storage,
        )

async def _get_dimension_id(
    name: str,
    version: Version,
    storage: ModelStore,
) -> dict:
    return await storage.get_dimension_id(name=name, version=version)


async def _get_dimension(
    id_: int,
    version: Version,
    storage: ModelStore,
) -> dict:
    return await storage.get_dimension(id_=id_, version=version)


async def _get_fact_id(
    name: str,
    version: Version,
    storage: ModelStore,
) -> dict:
    return await storage.get_fact_id(name=name, version=version)


async def _get_fact(
    id_: int,
    version: Version,
    storage: ModelStore,
) -> dict:
    return await storage.get_fact(id_=id_, version=version)


async def _update_model(
    dimensions: list[dict],
    facts: list[dict],
    read_version: Version,
    write_version: Version,
    dimension_structure_validator: DimensionStructureValidator,
    dimension_comparer: DimensionComparer,
    dimension_transformer: DimensionTransformer,
    fact_structure_validator: FactStructureValidator,
    fact_comparer: FactComparer,
    fact_transformer: FactTransformer,
    version_manager: VersionManager,
    storage: ModelStore,
):

    # This could throw a validation error
    for dimension in dimensions:
        dimension_structure_validator.validate(data=dimension)

    await storage.put_dimensions(read_version=read_version, write_version=write_version, dimensions=dimensions)

    ## This could throw a validation error
    #for fact in facts:
    #    fact_structure_validator.validate(data=fact)
    #
    #
    #
    ## TODO - the fact checking code is almost identical to the dimension checking code, refactor
    #facts_with_ids = {}
    #facts_without_ids = {}
    #
    #for fact in facts:
    #    fact_name = fact_transformer.get_name_from_fact(fact)
    #    try:
    #        # The fact may already have the id - if it is being edited
    #        id_ = fact_transformer.get_id_from_fact(fact)
    #        facts_with_ids[fact_name] = fact
    #    except KeyError:
    #        facts_without_ids[fact_name] = fact
    #
    ## Check for changes
    #fact_names_with_changes = list(facts_without_ids.keys())
    #for fact in facts_with_ids:
    #    id_ = fact_transformer.get_id_from_fact(fact)
    #
    #    previous_fact = await _get_fact(
    #        id_=id_, version=read_version, storage=storage
    #    )
    #    comparison = fact_comparer.compare(fact, previous_fact)
    #
    #    if comparison:
    #        fact_name = dimension_transformer.get_name_from_fact(fact)
    #        fact_names_with_changes.append(fact_name)

    # TODO - it would make sense to do basic validations and change detection BEFORE creating the new write version,
    #  Thus we would only have model_version flagged in the write version if we definitely needed one
    #  However, currently checks aren't done before write_version creation, so we will need to create indexes
    #  for dimensions and facts everytime



    #fact_version_index_read = await storage.get_fact_version_index(version=read_version)

    #try:
    #    dimension_id = await _get_dimension_id(
    #        name=dimension_name, version=write_version, storage=storage
    #    )
    #except KeyError:
    #
    #    try:
    #        dimension_id = await _get_dimension_id(
    #            name=dimension_name, version=read_version, storage=storage
    #        )
    #    except KeyError:
    #        # TODO - try cache then wrap this in its own get_next_dimension_id function
    #        dimension_id = await storage.get_max_dimension_id(write_version)
    #
    #    dimension_id += 1
    #
    #
    #    if not write_version.version:
    #        with await version_manager.write_model_version(
    #            read_version=read_version
    #        ) as new_write_version:
    #            await _check_and_put_dimension(
    #                dimension=dimension,
    #                read_version=read_version,
    #                write_version=new_write_version,
    #                storage=storage,
    #            )
    #    else:
    #        await _check_and_put_dimension(
    #            dimension=dimension,
    #            read_version=read_version,
    #            write_version=write_version,
    #            storage=storage,
    #        )
    #
    #    # TODO - notify changes - here? Or elsewhere?


async def _update_fact(
    fact: dict,
    read_version: Version,
    write_version: Version,
    fact_structure_validator: FactStructureValidator,
    fact_comparer: FactComparer,
    fact_transformer: FactTransformer,
    version_manager: VersionManager,
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

        if not write_version.version:
            with await version_manager.write_model_version(
                read_version=read_version
            ) as new_write_version:
                await _check_and_put_fact(
                    fact=fact,
                    read_version=read_version,
                    write_version=new_write_version,
                    storage=storage,
                )
        else:
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

    return await storage.put_dimension(
        dimension, read_version=read_version, write_version=write_version
    )


async def _check_and_put_fact(
    fact: dict, read_version: Version, write_version: Version, storage: ModelStore
):
    # TODO - check references - if we have made deletions we'll need to know
    # Pass the comparison into the reference check, and check references at the write version
    # Referred objects will be in

    return await storage.put_fact(
        fact, read_version=read_version, write_version=write_version
    )
