from lunch.base_classes.transformer import Transformer
from pandas import DataFrame, Series
from numpy import dtype
from typing import AsyncIterable, Iterable, Any

class DimensionDataFrameTransformer(Transformer):

    @staticmethod
    def make_dataframe(
            columns: dict[int, Iterable], dtypes: dict[int, dtype]
    ) -> DataFrame:
        """

        :param columns: dictionary of attribute id to Iterable of values, -1 for index
        :param dtypes: dictionary specifying the pandas dtype of each column
        :return: a pandas DataFrame made of the columns
        """


        series = {column_id: Series(data=iterable, dtype=dtypes.get(column_id, dtype("object"))) for column_id, iterable in columns.items()}

        return DataFrame(series)

    @staticmethod
    def merge(source_df: DataFrame, compare_df: DataFrame, key: list) -> DataFrame:
        pass

    @staticmethod
    def columnize(data: DataFrame) -> dict[int: Iterable]:
        # dictionary of columns? attribute_id : column/iterator
        # index is -1?
        pass
