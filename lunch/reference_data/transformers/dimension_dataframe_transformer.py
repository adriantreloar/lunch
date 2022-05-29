from lunch.base_classes.transformer import Transformer
from pandas import DataFrame
from typing import AsyncIterable, Iterable

class DimensionDataFrameTransformer(Transformer):

    @staticmethod
    async def make_dataframe(
            columns: list[AsyncIterable], index: AsyncIterable
    ) -> DataFrame:
        pass

    @staticmethod
    def merge(source_df: DataFrame, compare_df: DataFrame, key: list) -> DataFrame:
        pass

    @staticmethod
    def columnize(data: DataFrame) -> dict[int: Iterable]:
        # dictionary of columns? attribute_id : column/iterator
        # how to represent index? 0 or -1?
        pass
