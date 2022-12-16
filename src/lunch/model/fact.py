from pyrsistent import PClass, PRecord, CheckedPMap, CheckedPVector, field


class _FactDimensionMetadatum(PClass):
    name = field(type=str,
                 invariant=lambda x: (x != '', 'name empty'),
                 # initial=<object object>,
                 mandatory=True,
                 # factory=<function <lambda>>,
                 # serializer=<function <lambda>>
                 )

    view_order = field(type=int,
                mandatory=True,
                )

    column_id = field(type=int,
                invariant=lambda x: (x >= 0, 'column_id negative'),
                mandatory=True,
                )

class _FactMeasureMetadatum(PClass):
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
    __type__ = _FactDimensionMetadatum

class _FactMeasuresMetadata(CheckedPVector):
    __type__ = _FactMeasureMetadatum

class _ColumnIds(CheckedPVector):
    __type__ = int
    __invariant__ = lambda i: (i >= 0, 'column id not positive')

class FactStorage(PClass):

    index_columns = field(type=_ColumnIds,
                          invariant=lambda v: (len(set(v)) == len(v), 'column ids not unique'),
                          mandatory=True
                          )

    data_columns = field(type=_ColumnIds,
                         mandatory=True
                         )

class Fact(PClass):
    #__invariant__ = lambda r: (r.y >= r.x, 'x larger than y')

    name = field(type=str,
                 invariant=lambda x: (x != '', 'name empty'),
                 #initial=<object object>,
                 mandatory=True,
                 #factory=<function <lambda>>,
                 #serializer=<function <lambda>>
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
                       invariant=lambda v: (len({el.column_id for el in v})==len({el.name for el in v})==len(v), 'column_ids or names not unique'),
                       mandatory=True,
                       )

    measures = field(type=_FactMeasuresMetadata,
                     mandatory=True,
                     )

    storage = field(type=FactStorage,
                    mandatory=False,
                    )
