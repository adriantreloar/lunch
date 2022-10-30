from lunch.base_classes.constant import Constant


class GlobalState(Constant):
    @staticmethod
    @property
    def default_dimension_storage(self):
        return {"storage": "columnar"}
