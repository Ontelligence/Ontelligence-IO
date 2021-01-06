import dacite


class BaseDataClass:

    @classmethod
    def from_dict(cls, data):
        return dacite.from_dict(data_class=cls, data=data)
