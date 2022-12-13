from src.lunch.base_classes.constant import Constant


class GlobalState(Constant):
    default_dimension_storage = {"storage": "columnar"}
    default_fact_storage = {"storage": "columnar"}
