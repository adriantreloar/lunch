from pyrsistent import PClass, PRecord, CheckedPMap, CheckedPVector, field
from src.lunch.base_classes.transformer import Transformer
from typing import Any

class FactDimensionMetadatum(PClass):
    # TODO fix the invariant

    #__invariant__ = lambda m: ((False or m.name or m.column_id) and
    #                           (False or m.dimension_name or m.dimension_id)
    #                           , 'name or column_id must be set, dimension_name or dimension_id must be set')
    __invariant__ = lambda m: (True,
                               'name or column_id must be set, dimension_name or dimension_id must be set')

    name = field(type=str,
                 invariant=lambda x: (x != '', 'name empty'),
                 mandatory=False,
                 )

    view_order = field(type=int,
                mandatory=True,
                )

    column_id = field(type=int,
                invariant=lambda x: (x >= 0, 'column_id negative'),
                mandatory=False,
                )

    dimension_name = field(type=str,
                 invariant=lambda x: (x != '', 'name empty'),
                 mandatory=False,
                 )

    dimension_id = field(type=int,
                invariant=lambda x: (x >= 0, 'dimension_id negative'),
                mandatory=True,
                )

class FactMeasureMetadatum(PClass):
    name = field(type=str,
                 invariant=lambda x: (x != '', 'name empty'),
                 mandatory=True,
                 )

    measure_id = field(type=int,
                       invariant=lambda x: (x >= 0, 'id_ negative'),
                       mandatory=True,
                       )

    type = field(type=str,
                 invariant=lambda x: (x != '', 'type empty'),
                 mandatory=True,
                 )

    precision = field(type=int,
                      invariant=lambda x: (x >= 0, 'precision negative'),
                      mandatory=False,
                      )


class _FactDimensionsMetadata(CheckedPVector):
    __type__ = FactDimensionMetadatum

class _FactMeasuresMetadata(CheckedPVector):
    __type__ = FactMeasureMetadatum

class _ColumnIds(CheckedPVector):
    __type__ = int
    __invariant__ = lambda i: (i >= 0, 'column id not positive')

class FactStorage(PClass):

    index_columns = field(type=_ColumnIds,
                          invariant=lambda v: (len({c for c in v if c > 0}) == len([c for c in v if c > 0]), 'column ids not unique'),
                          mandatory=True
                          )

    data_columns = field(type=_ColumnIds,
                         mandatory=True
                         )

class Fact(PClass):

    name = field(type=str,
                 invariant=lambda x: (x != '', 'name empty'),
                 mandatory=True,
                 )

    fact_id = field(type=int,
                    invariant=lambda x: (x >= 0, 'fact_id negative'),
                    mandatory=False,
                    )

    model_version = field(type=int,
                          invariant=lambda x: (x >= 0, 'model_version negative'),
                          mandatory=False,
                          )

    dimensions = field(type=_FactDimensionsMetadata,
                       invariant=lambda v: (len({el.name for el in v})==len(v), 'names not unique'),
                       mandatory=True,
                       )

    measures = field(type=_FactMeasuresMetadata,
                     mandatory=True,
                     )

    storage = field(type=FactStorage,
                    mandatory=False,
                    )




class FactTransformer(Transformer):
    """Static methods to alter a fact"""

    # TODO - all of these change the dimensions, maybe we should expose a class that changes dimensions

    @staticmethod
    def to_dict(fact: Fact) -> dict:
        if fact is None:
            return None
        return fact.serialize()

    @staticmethod
    def from_dict(fact_dict: dict) -> Fact:
        if fact_dict is None:
            return None
        name = fact_dict["name"]
        fact_id = fact_dict["fact_id"]
        model_version = fact_dict["model_version"]
        dimensions = fact_dict["dimensions"]
        measures = fact_dict["measures"]
        storage = fact_dict["storage"]
        return Fact(
            name=name,
            fact_id=fact_id,
            model_version=model_version,
            dimensions=dimensions,
            #field(type=_FactDimensionsMetadata,
            #                   invariant=lambda v: (len({el.name for el in v}) == len(v), 'names not unique'),
            #                   mandatory=True,
            #                   )
            measures=measures,
            #field(type=_FactMeasuresMetadata,
            #                 mandatory=True,
            #                 )
            storage=storage
            #field(type=FactStorage,
            #                mandatory=False,
            #                )
            )

    @staticmethod
    def get_max_view_order(fact: Fact) -> int:
        if not fact.dimensions:
            return 0
        else:
            return max([d_meta.view_order for d_meta in fact.dimensions])

    @staticmethod
    def get_max_column_id(fact: Fact) -> int:
        if not fact.dimensions:
            return 0
        else:
            return max([d_meta.column_id for d_meta in fact.dimensions])

    @staticmethod
    def is_view_order_set_on_all_dimensions(fact: Fact) -> bool:
        return all([d_meta.view_order for d_meta in fact.dimensions])

    @staticmethod
    def is_column_id_set_on_all_dimensions(fact: Fact) -> bool:
        return all([d_meta.column_id for d_meta in fact.dimensions])



    @staticmethod
    def fill_default_view_order(fact: Fact) -> Fact:
        """
        :param fact: A Fact containing partial data
        :return: A Fact with view order defaulted
        """

        if not fact.dimensions or FactTransformer.is_view_order_set_on_all_dimensions(fact):
            return fact
        else:
            new_dimensions = []
            max_view_order = FactTransformer.get_max_view_order(fact)
            for d_meta in fact.dimensions:
                if not d_meta.view_order:
                    max_view_order += 1
                    d_meta = d_meta.set(view_order=max_view_order)
                new_dimensions.append(d_meta)

            return fact.set(dimensions=new_dimensions)

    @staticmethod
    def fill_default_column_ids(fact: Fact) -> Fact:
        """
        :param fact: A Fact containing partial data
        :return: A Fact with column id defaulted
        """

        if not fact.dimensions or FactTransformer.is_column_id_set_on_all_dimensions(fact):
            return fact
        else:
            new_dimensions = []
            max_column_id = FactTransformer.get_max_column_id(fact)
            for d_meta in fact.dimensions:
                if not d_meta.column_id:
                    max_column_id += 1
                    d_meta = d_meta.set(column_id=max_column_id)
                new_dimensions.append(d_meta)

            return fact.set(dimensions=new_dimensions)

    @staticmethod
    def fill_dimension_info(fact: Fact, dimensions: list[dict]) -> Fact:
        """
        :param fact: A Fact containing partial dimension data
        :param dimensions: full, canonical, dimensions from storage which we can use to fill missing details
        :return: A Fact with column id defaulted
        """
        if not fact.dimensions:
            return fact
        else:
            dimensions_by_id = {d["id_"]: d for d in dimensions}
            dimensions_by_name = {d["name"]: d for d in dimensions}

            new_dimension_metas = []
            for d_meta in fact.dimensions:
                if d_meta.dimension_id:
                    canonical_dim = dimensions_by_id[d_meta.dimension_id]
                    d_meta = d_meta.set(dimension_name=canonical_dim["name"])
                else:
                    canonical_dim = dimensions_by_name[d_meta.dimension_name]
                    d_meta = d_meta.set(dimension_id=canonical_dim["id_"])

                new_dimension_metas.append(d_meta)

            return fact.set(dimensions=new_dimension_metas)

    @staticmethod
    def get_unique_dimension_ids(fact: Fact) -> set[int]:
        return {d_meta.dimension_id for d_meta in fact.dimensions if d_meta.dimension_id}
