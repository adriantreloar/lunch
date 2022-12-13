from typing import Iterable, Mapping

import numpy as np
import pandas as pd

from src.lunch.base_classes.transformer import Transformer


class FactDataFrameTransformer(Transformer):
    @staticmethod
    def make_dataframe(
        columns: Mapping[int, Iterable], dtypes: Mapping[int, np.dtype]
    ) -> pd.DataFrame:
        """

        :param columns: dictionary of attribute id to Iterable of values, -1 for index
        :param dtypes: dictionary specifying the pandas dtype of each column
        :return: a pandas DataFrame made of the columns
        """

        series = {
            column_id: pd.Series(
                data=iterable, dtype=dtypes.get(column_id, np.dtype("object"))
            )
            for column_id, iterable in columns.items()
        }

        return pd.DataFrame(series)

    @staticmethod
    def merge(
        source_df: pd.DataFrame, compare_df: pd.DataFrame, key: list
    ) -> pd.DataFrame:
        col_names = pd.Index(
            np.concatenate([source_df.columns, compare_df.columns])
        ).drop_duplicates()

        print()
        print(__file__)
        print(compare_df)

        # TODO, been a bit sloppy here with the merge key
        #  I am sure a test will show that it is failing
        #  as merges haven't been properly written yet
        df = (
            source_df.set_index(key)
            .combine_first(compare_df.set_index(key))
            .reset_index()
            .reindex(columns=col_names)  # type: ignore
        )

        return df

    @staticmethod
    def columnize(data: pd.DataFrame) -> dict[int, Iterable]:
        # dictionary of columns? attribute_id : column/iterator
        # index is -1?

        output = {}
        for col in data.columns:
            output[col] = data[col].tolist()

        return output
