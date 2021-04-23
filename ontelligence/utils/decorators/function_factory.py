
from typing import Union, List, Callable, TypeVar, cast

from functools import wraps
from inspect import signature


def provide_if_missing(fields: Union[str, List[str]]):

    if isinstance(fields, str):
        fields = [fields]

    t = TypeVar('t', bound=Callable)

    def decorator_func(func: t) -> t:
        """
        Function decorator that provides values used during instantiation if not passed to the function.
        """
        function_signature = signature(func)

        @wraps(func)
        def wrapper(*args, **kwargs) -> t:
            bound_args = function_signature.bind(*args, **kwargs)

            for each_field in fields:
                if each_field not in bound_args.arguments:
                    self = args[0]
                    bound_args.arguments[each_field] = getattr(self, each_field)

            return func(*bound_args.args, **bound_args.kwargs)
        return cast(t, wrapper)

    return decorator_func
