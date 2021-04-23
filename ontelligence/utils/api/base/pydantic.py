from pydantic import BaseModel, validator, root_validator
import dacite


# TODO: Currently using pydantic.BaseModel in APIs.
# class BaseSchema(BaseModel):
#
#     @classmethod
#     def from_dict(cls, data):
#         return dacite.from_dict(data_class=cls, data=data)


def ensure_unique_identity(model, field):
    # print('kwargs:', kwargs)
    print('field:', field)
    decorator = validator(field, allow_reuse=True)

    def f(cls, data):
        print('data:', data)
        return data

    return decorator(f)


# def ensure_unique_identity():
#     print('kwargs:', kwargs)
#     decorator = root_validator(pre=True, allow_reuse=True)
#
#     def f(cls, values):
#         print('values:', values)
#         return values
#
#     return decorator(f)
