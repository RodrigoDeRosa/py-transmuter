from typing import Any, Callable, Generic, Mapping, TypeVar, get_args

from pydantic import BaseModel

from py_transmuter.pydantic.utils import get_required_fields
from py_transmuter.self_inspector import SelfInspector

SourceModel = TypeVar("SourceModel", bound=BaseModel)
TargetModel = TypeVar("TargetModel", bound=BaseModel)


class BaseModelMapper(Generic[TargetModel, SourceModel], SelfInspector):
    """
    A generic mapper class that maps data from a source model to a target model.

    Attributes:
        mapping (Mapping[str, str | tuple[str, Callable[[Any], Any]] | Callable[[SourceModel], Any]):
            A mapping dictionary that defines the mapping between the fields of
            the source model and the target model.
            These mappings can be:
                - A string that represents the name of the field in the source model
                - A tuple with the name of a field in the source model and a callable
                    that takes the value of the field in the source model and returns
                    the value of the field in the target model.
                - A callable that takes the source model as an argument and returns
                    the value of the field in the target model.
        context (Mapping[str, Any] | None):
            A dictionary that allows passing specific attributes to the aggregator
            if they are instance specific not class wide. Defaults to None.

    Methods:
        map(data: SourceModel) -> TargetModel:
            Maps the data from the source model to a target model using the
            mapping dictionary.
        map_list(data: list[SourceModel]) -> list[TargetModel]:
            Maps a list of source models to a list of target models, one by one,
            in order, using the mapping dictionary.

    Raises:
        ValueError:
            If a compulsory field in the target model does not have a mapping
            in the mapper.
        ValueError:
            If a field in the mapper does not exist in the target model.
    """

    mapping: Mapping[
        str, str | tuple[str, Callable[[Any], Any]] | Callable[[SourceModel], Any]
    ]

    context: Mapping[str, Any] | None = None

    def __init__(self, context: Mapping[str, Any] | None = None) -> None:
        self.assert_is_valid_mapper()

        self.context = context

    def map_list(self, data: list[SourceModel]) -> list[TargetModel]:
        return [self.map(item) for item in data]

    def map(self, data: SourceModel) -> TargetModel:
        resolved_map = {
            target_field_name: self.resolve_field(data, source_field)
            for target_field_name, source_field in self.mapping.items()
        }
        return self.target_model()(**resolved_map)

    def resolve_field(
        self,
        external_model: SourceModel,
        mapping_field: (
            str | tuple[str, Callable[[Any], Any]] | Callable[[SourceModel], Any]
        ),
    ) -> Any:
        if isinstance(mapping_field, str):
            return getattr(external_model, mapping_field)

        if isinstance(mapping_field, tuple):
            source_field, callable = mapping_field
            return self.resolve_callable(callable)(
                getattr(external_model, source_field)
            )

        return self.resolve_callable(mapping_field)(external_model)

    @classmethod
    def target_model(cls) -> type[TargetModel]:
        return get_args(cls.__orig_bases__[-1])[0]

    def assert_is_valid_mapper(self) -> None:
        """Asserts that the mapper won't fail when trying to build an instance of the target model."""
        if getattr(self, "mapping", None) is None:
            raise ValueError("The mapper must define a mapping dictionary.")

        target_model = self.target_model()

        required_fields = set(get_required_fields(target_model))
        defined_mappings = set(self.mapping.keys())

        missing_required = required_fields.difference(defined_mappings)
        if missing_required:
            fields = ", ".join(missing_required)
            raise ValueError(
                f"Fields {fields} in target model {target_model.__name__} "
                "do not have a mapping in the mapper."
            )

        all_fields = set(target_model.__annotations__.keys())
        extra_fields = defined_mappings.difference(all_fields)
        if extra_fields:
            fields = ", ".join(extra_fields)
            raise ValueError(
                f"Fields {fields} in the mapper do not exist "
                f"in target model {target_model.__name__}."
            )
