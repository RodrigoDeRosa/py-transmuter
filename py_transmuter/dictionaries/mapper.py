from typing import Any, Callable, Mapping
from py_transmuter.self_inspector import SelfInspector


class DictionaryMapper(SelfInspector):
    """
    A generic mapper class that maps data from a source model to a target model.

    Attributes:
        mapping (Mapping[str, str | tuple[str, Callable[[Any], Any]] | Callable[[dict[Any, Any]], Any]):
            A mapping dictionary that defines the mapping between two dictionaries.
            These mappings can be:
                - A string that represents the name of a field in the source dictionary
                - A tuple with the name of a field in the source dictionary and a callable
                    that takes the value of the field in the source dictionary and returns
                    the value of the field in the target dictionary.
                - A callable that takes the source dictionary as an argument and returns
                    the value of the field in the target dictionary.
        context (Mapping[str, Any] | None):
            A dictionary that allows passing specific attributes to the mapper
            if they are instance specific not class wide. Defaults to None.

    Methods:
        map(data: dict[Any, Any]) -> dict[Any, Any]:
            Maps the data from the source dictionary to another dictionary using the
            mapping instructions.
        map_list(data: list[dict[Any, Any]]) -> list[dict[Any, Any]]:
            Maps a list of dictionaries to another list of dictionaries, one by one,
            in order, using the mapping instructions.

    Raises:
        ValueError:
            If the mapper doesn't have a mapping dictionary.
    """

    mapping: Mapping[
        Any, Any | tuple[Any, Callable[[Any], Any]] | Callable[[dict[Any, Any]], Any]
    ]

    context: Mapping[str, Any] | None = None

    def __init__(self, context: Mapping[str, Any] | None = None) -> None:
        self.assert_is_valid_mapper()

        self.context = context

    def map_list(self, data: list[dict[Any, Any]]) -> list[dict[Any, Any]]:
        return [self.map(item) for item in data]

    def map(self, data: dict[Any, Any]) -> dict[Any, Any]:
        return {
            target_field_name: self.resolve_field(data, source_field)
            for target_field_name, source_field in self.mapping.items()
        }
    
    def resolve_field(
        self,
        external_data: dict[Any, Any],
        mapping_field: (
            Any | tuple[Any, Callable[[Any], Any]] | Callable[[dict[Any, Any]], Any]
        ),
    ) -> Any:
        if isinstance(mapping_field, tuple):
            source_field, mapper_function = mapping_field
            return self.resolve_callable(mapper_function)(external_data[source_field])
        
        if callable(mapping_field) or isinstance(mapping_field, classmethod):
            return self.resolve_callable(mapping_field)(external_data)

        return external_data[mapping_field]
    
    def assert_is_valid_mapper(self) -> None:
        """Asserts that the mapper won't fail when trying to build an instance of the target model."""
        if getattr(self, "mapping", None) is None:
            raise ValueError("The mapper must define a mapping dictionary.")