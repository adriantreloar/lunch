import pandas as pd

from lunch.base_classes.transformer import Transformer


class DimensionDataFrameMerger(Transformer):
    def __init__(
        self, source_df: pd.DataFrame, compare_df: pd.DataFrame, dimension_model: dict
    ) -> None:
        self._source_dataframe: pd.DataFrame = source_df
        self._compare_dataframe: pd.DataFrame = compare_df
        self._dimension_model: dict = dimension_model

    def merge(self, key: list[str]) -> pd.DataFrame:
        return _merge(
            source_df=self._source_dataframe,
            compare_df=self._compare_dataframe,
            key=key,
        )


def _merge(
    source_df: pd.DataFrame, compare_df: pd.DataFrame, key: list[str]
) -> pd.DataFrame:
    # TODO - note - this does no merge as yet, and doesn't even add the surrogate key
    # it is not ready to go
    # as a minimum add the surrogate key, so we can store brand new dimensional data
    return source_df.copy()
