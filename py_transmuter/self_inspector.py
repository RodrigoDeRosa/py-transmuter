from functools import partial
import inspect
from typing import Any, Callable

AnyCallable = Callable[..., Any]


class SelfInspector:
    """
    This class provides a utility method `resolve_callable` that can be used to
    determine whether a callable belongs to the instance itself and returns another
    callable, with the instance bound to it.
    """

    def resolve_callable(self, callable: AnyCallable) -> AnyCallable:
        if self.is_instance_method(callable):
            return partial(callable, self)
        elif self.is_class_method(callable):
            return partial(callable.__func__, self.__class__)
        elif self.is_static_method(callable):
            return callable.__func__

        return callable

    @classmethod
    def is_instance_method(cls, callable: AnyCallable) -> bool:
        functions = [
            function for _, function in inspect.getmembers(cls, inspect.isfunction)
        ]
        return callable in functions

    @classmethod
    def is_class_method(cls, callable: AnyCallable) -> bool:
        methods = [
            method.__func__ for _, method in inspect.getmembers(cls, inspect.ismethod)
        ]
        return isinstance(callable, classmethod) and callable.__func__ in methods

    @classmethod
    def is_static_method(cls, callable: AnyCallable) -> bool:
        functions = [
            function for _, function in inspect.getmembers(cls, inspect.isfunction)
        ]
        return isinstance(callable, staticmethod) and callable.__func__ in functions
