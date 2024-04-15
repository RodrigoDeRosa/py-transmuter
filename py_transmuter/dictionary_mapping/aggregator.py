from collections import defaultdict
from typing import (
    Any,
    Callable,
    Mapping,
)
from py_transmuter.self_inspector import SelfInspector


class DictionaryAggregator(SelfInspector):
    """
    Aggregator class that transforms a list of objects of the source model
    into a list of objects of the target model.

    Attributes:
        group_by (tuple[Any | Callable[[dict[Any, Any]], Any], ...] | None):
            The fields to group the data by or methods that can extract a value to group by
            from the source dictionary. Defaults to None.
        sort_by (tuple[Any | Callable[[dict[Any, Any]], Any], ...] | None):
            The fields to sort the data by or methods that can extract a value to sort by
            from the source dictionary. Defaults to None.
        mappings (Mapping[Any, Any | tuple[Any, Callable[[Any], Any]] | Callable[[SourceModel], Any]] | None):
            The mappings to apply to each individual dictionary in a group of data. These mappings
            always result in a list with values of Any type of the same length as the group.
            A mapping can be:
                - A string that represents the name of the field in the source dictionary, this
                    returns a list containing the value itself for each instance in the group,
                    in `sort_by` order.
                - A tuple with a key of the source dictionary and a callable that takes the value 
                    linked to that key in the source dictionary and returns the value of the field 
                    in the target dictionary. This returns a list with each value in the group 
                    mapped, in `sort_by` order.
                - A callable that takes the source dictionary as an argument and returns
                    the value of the field in the target dictionary. This returns a list
                    with each instance in the group mapped, in `sort_by` order.
        aggregations (Mapping[Any,tuple[Any, Callable[[list[Any]], Any]] | Callable[[list[dict[Any, Any]]], Any]] | None):
            The aggregations to apply to each group of data. The values of the
            dictionary can be:
                - A tuple with the name of a field in the source dictionary and a callable
                    that receives a list of all the values of the field in the source
                    dictionary instances in the group and returns the value of the field in
                    the target dictionary. The callable will always receive the values of the
                    field of each source dictionary instance in `sort_by` order.
                - A callable that takes a list of objects of the source dictionary as an
                    argument and returns the value of the field in the target dictionary. The
                    callable will always receive the instances in `sort_by` order.
        context (Mapping[str, Any] | None):
            A dictionary that allows passing specific attributes to the aggregator
            if they are instance specific not class wide. Defaults to None.

    Methods:
        aggregate(data: list[dict[Any, Any]]) -> list[dict[Any, Any]]:
            Aggregates the source models into target models.

    Raises:
        ValueError:
            If a field in the target model does not have a mapping in the aggregator.
        ValueError:
            If a field in the aggregator does not exist in the target model.
    """

    group_by: tuple[Any | Callable[[dict[Any, Any]], Any], ...] | None = None
    sort_by: tuple[Any | Callable[[dict[Any, Any]], Any], ...] | None = None

    mappings: (
        Mapping[
            Any,
            Any | tuple[str, Callable[[Any], Any]] | Callable[[dict[Any, Any]], Any],
        ]
        | None
    ) = None

    aggregations: (
        Mapping[
            Any,
            tuple[Any, Callable[[list[Any]], Any]]
            | Callable[[list[dict[Any, Any]]], Any],
        ]
        | None
    ) = None

    context: Mapping[str, Any] | None = None

    def __init__(self, context: Mapping[str, Any] | None = None) -> None:
        self.assert_is_valid_aggregator()

        self.context = context

    def aggregate(self, data: list[dict[Any, Any]]) -> list[dict[Any, Any]]:
        if self.sort_by is not None:
            data = self.sort_data(data)

        groups = self.group_data(data) if self.group_by is not None else [data]

        return [self.resolve_group_objects(group) for group in groups]

    def sort_data(self, data: list[dict[Any, Any]]) -> list[dict[Any, Any]]:
        return sorted(
            data,
            key=lambda item: tuple(
                (
                    self.resolve_callable(field)(item)
                    if callable(field) or isinstance(field, classmethod)
                    else item[field]
                )
                for field in self.sort_by
            ),
        )

    def group_data(self, data: list[dict[Any, Any]]) -> list[list[dict[Any, Any]]]:
        groups = defaultdict(list)

        for item in data:
            group = tuple(
                (
                    self.resolve_callable(field)(item)
                    if callable(field) or isinstance(field, classmethod)
                    else item[field]
                )
                for field in self.group_by
            )
            groups[group].append(item)

        return list(groups.values())

    def resolve_group_objects(self, group: list[dict[Any, Any]]) -> dict[Any, Any]:
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
            tuple[Any, Callable[[list[Any]], Any]]
            | Callable[[list[dict[Any, Any]]], Any]
        ),
        group: list[dict[Any, Any]],
    ) -> list[Any]:
        if isinstance(aggregation, tuple):
            source_field, callable = aggregation
            return self.resolve_callable(callable)(
                [item[source_field] for item in group]
            )

        return self.resolve_callable(aggregation)(group)

    def resolve_mapping(
        self,
        mapping_field: (
            Any | tuple[Any, Callable[[Any], Any]] | Callable[[dict[Any, Any]], Any]
        ),
        external_data: dict[Any, Any],
    ) -> Any:
        if isinstance(mapping_field, tuple):
            source_field, mapper_function = mapping_field
            return self.resolve_callable(mapper_function)(external_data[source_field])
        
        if callable(mapping_field) or isinstance(mapping_field, classmethod):
            return self.resolve_callable(mapping_field)(external_data)

        return external_data[mapping_field]

    def assert_is_valid_aggregator(self) -> None:
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
