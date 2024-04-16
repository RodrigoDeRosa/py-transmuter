from typing import Any, Protocol, TypeVar, runtime_checkable


class ConstructableFromDict(Protocol):
    """Protocol for any class that can be constructed from a dictionary."""
    def __init__(self, **kwargs: Any) -> None:
        ...


@runtime_checkable
class SupportsTypeHints(Protocol):
    """Protocol for any class from which type hints can be extracted."""
    pass


class MappableModel(ConstructableFromDict, SupportsTypeHints, Protocol):
    """Protocol for any class that can be used as a source or target model."""
    pass
    

SourceModel = TypeVar("SourceModel", bound=ConstructableFromDict)
TargetModel = TypeVar("TargetModel", bound=MappableModel)