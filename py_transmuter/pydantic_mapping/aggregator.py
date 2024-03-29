from collections import defaultdict
from typing import (
    Any,
    Callable,
    Generic,
    Mapping,
    TypeVar,
    get_args,
)

from pydantic import BaseModel
from py_transmuter.pydantic_mapping.utils import get_required_fields
from py_transmuter.self_inspector import SelfInspector


SourceModel = TypeVar("SourceModel", bound=BaseModel)
TargetModel = TypeVar("TargetModel", bound=BaseModel)


class BaseModelAggregator(Generic[TargetModel, SourceModel], SelfInspector):
    """
    Aggregator class that transforms a list of objects of the source model
    into a list of objects of the target model.

    Attributes:
        group_by (tuple[str | Callable[[SourceModel], Any], ...] | None):
            The fields to group the data by or methods that can extract a value to group by
            from the source model. Defaults to None.
        sort_by (tuple[str | Callable[[SourceModel], Any], ...] | None):
            The fields to sort the data by or methods that can extract a value to sort by
            from the source model. Defaults to None.
        mappings (Mapping[str, str | tuple[str, Callable[[Any], Any]] | Callable[[SourceModel], Any]] | None):
            The mappings to apply to each individual object in a group of data. These mappings
            always result in a list with values of Any type of the same length as the group.
            A mapping can be:
                - A string that represents the name of the field in the source model, this
                    returns a list containing the value itself for each instance in the group,
                    in `sort_by` order.
                - A tuple with the name of a field in the source model and a callable
                    that takes the value of the field in the source model and returns
                    the value of the field in the target model. This returns a list with
                    each value in the group mapped, in `sort_by` order.
                - A callable that takes the source model as an argument and returns
                    the value of the field in the target model. This returns a list
                    with each instance in the group mapped, in `sort_by` order.
        aggregations (Mapping[str,tuple[str, Callable[[list[Any]], Any]] | Callable[[list[SourceModel]], Any]] | None):
            The aggregations to apply to each group of data. The values of the
            dictionary can be:
                - A tuple with the name of a field in the source model and a callable
                    that receives a list of all the values of the field in the source
                    model instances in the group and returns the value of the field in
                    the target model. The callable will always receive the values of the
                    field of each source model instance in `sort_by` order.
                - A callable that takes a list of objects of the source model as an
                    argument and returns the value of the field in the target model. The
                    callable will always receive the instances in `sort_by` order.
        context (Mapping[str, Any] | None):
            A dictionary that allows passing specific attributes to the aggregator
            if they are instance specific not class wide. Defaults to None.

    Methods:
        aggregate(data: list[SourceModel]) -> list[TargetModel]:
            Aggregates the source models into target models.

    Raises:
        ValueError:
            If a field in the target model does not have a mapping in the aggregator.
        ValueError:
            If a field in the aggregator does not exist in the target model.
    """

    group_by: tuple[str | Callable[[SourceModel], Any], ...] | None = None
    sort_by: tuple[str | Callable[[SourceModel], Any], ...] | None = None

    mappings: (
        Mapping[
            str, str | tuple[str, Callable[[Any], Any]] | Callable[[SourceModel], Any]
        ]
        | None
    ) = None

    aggregations: (
        Mapping[
            str,
            tuple[str, Callable[[list[Any]], Any]] | Callable[[list[SourceModel]], Any],
        ]
        | None
    ) = None

    context: Mapping[str, Any] | None = None

    def __init__(self, context: Mapping[str, Any] | None = None) -> None:
        self.assert_is_valid_aggregator()

        self.context = context

    def aggregate(self, data: list[SourceModel]) -> list[TargetModel]:
        if self.sort_by is not None:
            data = self.sort_data(data)

        groups = self.group_data(data) if self.group_by is not None else [data]

        resolved_objects = [self.resolve_group_objects(group) for group in groups]

        return [self.target_model()(**obj) for obj in resolved_objects]

    def sort_data(self, data: list[SourceModel]) -> list[SourceModel]:
        return sorted(
            data,
            key=lambda item: tuple(
                (
                    getattr(item, field)
                    if isinstance(field, str)
                    else self.resolve_callable(field)(item)
                )
                for field in self.sort_by
            ),
        )

    def group_data(self, data: list[SourceModel]) -> list[list[SourceModel]]:
        groups = defaultdict(list)

        for item in data:
            group = tuple(
                (
                    getattr(item, field)
                    if isinstance(field, str)
                    else self.resolve_callable(field)(item)
                )
                for field in self.group_by
            )
            groups[group].append(item)

        return list(groups.values())

    def resolve_group_objects(self, group: list[SourceModel]) -> dict[str, Any]:
        aggregated_fields = (
            {
                target_field_name: self.resolve_aggregation(aggregation, group)
                for target_field_name, aggregation in self.aggregations.items()
            }
            if self.aggregations is not None
            else dict()
        )

        mapped_fields = (
            {
                target_field_name: [
                    self.resolve_mapping(mapping, item) for item in group
                ]
                for target_field_name, mapping in self.mappings.items()
            }
            if self.mappings is not None
            else dict()
        )

        return {**aggregated_fields, **mapped_fields}

    def resolve_aggregation(
        self,
        aggregation: (
            tuple[str, Callable[[list[Any]], Any]] | Callable[[list[SourceModel]], Any]
        ),
        group: list[SourceModel],
    ) -> list[Any]:
        if isinstance(aggregation, tuple):
            source_field, callable = aggregation
            return self.resolve_callable(callable)(
                [getattr(item, source_field) for item in group]
            )

        return self.resolve_callable(aggregation)(group)

    def resolve_mapping(
        self,
        mapping_field: (
            str | tuple[str, Callable[[Any], Any]] | Callable[[SourceModel], Any]
        ),
        external_model: SourceModel,
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

    def assert_is_valid_aggregator(self) -> None:
        """Asserts that the aggregator won't fail when trying to build an instance of the target model."""
        if (
            getattr(self, "aggregations", None) is None
            and getattr(self, "mappings", None) is None
        ):
            raise ValueError(
                "Aggregator must have either aggregations or mappings attribute."
            )

        defined_aggregations = (
            set() if self.aggregations is None else set(self.aggregations.keys())
        )
        defined_mappings = set() if self.mappings is None else set(self.mappings.keys())

        double_definitions = defined_mappings.intersection(defined_aggregations)
        if double_definitions:
            fields = ", ".join(double_definitions)
            raise ValueError(
                f"Fields {fields} are mapped both in the mappings"
                "and the aggregations definitions. This is not permitted."
            )

        all_defined_fields = defined_aggregations.union(defined_mappings)

        target_model = self.target_model()
        required_fields = set(get_required_fields(target_model))

        missing_required = required_fields.difference(all_defined_fields)
        if missing_required:
            fields = ", ".join(missing_required)
            raise ValueError(
                f"Fields {fields} in the target model {target_model.__name__} "
                "do not have a mapping or aggregation in the aggregator."
            )

        all_fields = set(target_model.__annotations__.keys())
        extra_fields = all_defined_fields.difference(all_fields)
        if extra_fields:
            fields = ", ".join(extra_fields)
            raise ValueError(
                f"Fields {fields} in the aggregator do not exist "
                f"in the target model {target_model.__name__}."
            )
