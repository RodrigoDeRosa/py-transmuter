from datetime import date, datetime, timedelta
from statistics import mean
from typing import Any

from pytest import raises
from py_transmuter.dictionary_mapping.aggregator import DictionaryAggregator


def test_aggregator_field_direct_mappings():
    class Aggregator(DictionaryAggregator):
        mappings = {"ids": "id"}

    assert Aggregator().aggregate([{"id": 1}, {"id": 2}, {"id": 3}]) == [
        {"ids": [1, 2, 3]}
    ]


def test_aggregator_tuple_direct_mappings():
    class Aggregator(DictionaryAggregator):
        mappings = {"ids": ("id", lambda id: str(id))}

    assert Aggregator().aggregate([{"id": 1}, {"id": 2}, {"id": 3}]) == [
        {"ids": ["1", "2", "3"]}
    ]


def test_aggregator_model_callable_direct_mappings():
    class Aggregator(DictionaryAggregator):
        @staticmethod
        def make_full_name(data: dict[Any, Any]) -> str:
            return f"{data['first_name']} {data['last_name']}"

        mappings = {"full_names": make_full_name}

    assert Aggregator().aggregate(
        [
            {"first_name": "Rodrigo", "last_name": "De Rosa"},
            {"first_name": "Marcelo", "last_name": "Gallardo"},
            {"first_name": "Super", "last_name": "Man"},
        ]
    ) == [{"full_names": ["Rodrigo De Rosa", "Marcelo Gallardo", "Super Man"]}]


def test_aggregator_field_direct_mappings_with_grouping():
    class Aggregator(DictionaryAggregator):
        group_by = ("parent",)

        @staticmethod
        def make_full_name(data: dict[Any, Any]):
            return f"{data['first_name']} {data['last_name']}"

        mappings = {"children_names": make_full_name}

    assert Aggregator().aggregate(
        [
            {"parent": "Patricio", "first_name": "Rodrigo", "last_name": "De Rosa"},
            {"parent": "God Himself", "first_name": "Marcelo", "last_name": "Gallardo"},
            {"parent": "God Himself", "first_name": "Super", "last_name": "Man"},
        ]
    ) == [
        {"children_names": ["Rodrigo De Rosa"]},
        {"children_names": ["Marcelo Gallardo", "Super Man"]},
    ]


def test_aggregator_group_by_with_extractor():
    class Aggregator(DictionaryAggregator):
        @staticmethod
        def parent_full_name(data: dict[Any, Any]) -> str:
            return f"{data['parent']} {data['last_name']}"

        group_by = (parent_full_name,)

        mappings = {"children_names": "first_name"}

    assert Aggregator().aggregate(
        [
            {"parent": "Tom", "first_name": "Paul", "last_name": "Doe"},
            {"parent": "Tom", "first_name": "George", "last_name": "Stones"},
            {"parent": "Tom", "first_name": "Carl", "last_name": "Doe"},
        ]
    ) == [{"children_names": ["Paul", "Carl"]}, {"children_names": ["George"]}]


def test_aggregator_with_grouping_by_field_and_extractor():
    class Aggregator(DictionaryAggregator):
        @staticmethod
        def timestamp_date(data: dict[Any, Any]) -> date:
            return data["timestamp"].date()

        group_by = ("machine", timestamp_date)

        mappings = {"intervals": "timestamp", "values": "value"}

    aggregator = Aggregator()
    day_values = aggregator.aggregate(
        [
            {
                "machine": "A",
                "timestamp": datetime(2024, 2, 16) + timedelta(hours=12 * i),
                "value": 100,
            }
            for i in range(4)
        ]
    )

    assert day_values == [
        {
            "intervals": [datetime(2024, 2, 16), datetime(2024, 2, 16, 12)],
            "values": [100, 100],
        },
        {
            "intervals": [datetime(2024, 2, 17), datetime(2024, 2, 17, 12)],
            "values": [100, 100],
        },
    ]


def test_aggregator_with_sort_by_field():
    class Aggregator(DictionaryAggregator):
        sort_by = ("id",)

        mappings = {"ids": "id"}

    assert Aggregator().aggregate([{"id": 3}, {"id": 1}, {"id": 2}]) == [
        {"ids": [1, 2, 3]}
    ]


def test_aggregator_with_sort_by_extractor():
    def make_full_name(data: dict[Any, Any]) -> str:
        return f"{data['first_name']} {data['last_name']}"

    class Aggregator(DictionaryAggregator):
        sort_by = (make_full_name,)

        mappings = {"children_names": make_full_name}

    assert Aggregator().aggregate(
        [
            {"first_name": "Paul", "last_name": "Smith"},
            {"first_name": "Paul", "last_name": "Holmes"},
        ]
    ) == [{"children_names": ["Paul Holmes", "Paul Smith"]}]


def test_aggregator_with_sort_by_extractor_and_field():
    def make_full_name(data: dict[Any, Any]) -> str:
        return f"{data['first_name']} {data['last_name']}"

    class Aggregator(DictionaryAggregator):
        sort_by = (make_full_name, "age")

        mappings = {"children_names": make_full_name, "ages": "age"}

    assert Aggregator().aggregate(
        [
            {"first_name": "Paul", "last_name": "Smith", "age": 17},
            {"first_name": "Paul", "last_name": "Holmes", "age": 23},
            {"first_name": "Paul", "last_name": "Smith", "age": 5},
        ]
    ) == [
        {
            "children_names": ["Paul Holmes", "Paul Smith", "Paul Smith"],
            "ages": [23, 5, 17],
        }
    ]


def test_aggregator_with_field_mapper_aggregations():
    def aggregate_values(values: list[float]) -> float:
        average = sum(values) / len(values)
        return [value / average for value in values]

    class Aggregator(DictionaryAggregator):
        aggregations = {
            "values": ("value", aggregate_values),
        }

    assert Aggregator().aggregate([{"value": 6}, {"value": 4}, {"value": 2}]) == [
        {"values": [1.5, 1, 0.5]}
    ]


def test_aggregator_with_extractor_aggregations():
    def average_coordinates(turbines: list[dict[Any, Any]]) -> dict[Any, Any]:
        return {
            "latitude": mean(turbine["latitude"] for turbine in turbines),
            "longitude": mean(turbine["longitude"] for turbine in turbines),
        }

    class Aggregator(DictionaryAggregator):
        aggregations = {
            "location": average_coordinates,
        }

    assert Aggregator().aggregate(
        [
            {"latitude": 10, "longitude": 5},
            {"latitude": 15, "longitude": 10},
            {"latitude": 20, "longitude": 0},
        ]
    ) == [{"location": {"latitude": 15, "longitude": 5}}]


def test_aggregator_with_mappings_and_aggregations():
    class Aggregator(DictionaryAggregator):
        group_by = ("parent_id",)

        mappings = {"values": "value"}
        aggregations = {"id": ("parent_id", lambda ids: ids[0])}

    assert Aggregator().aggregate(
        [
            {"parent_id": 1, "value": 10},
            {"parent_id": 1, "value": 20},
            {"parent_id": 2, "value": 30},
        ]
    ) == [{"id": 1, "values": [10, 20]}, {"id": 2, "values": [30]}]


def test_aggregator_with_overlapping_mappings_and_aggregations_fails_to_create():
    class Aggregator(DictionaryAggregator):
        mappings = {"id": "parent_id"}
        aggregations = {"id": ("parent_id", lambda ids: ids[0])}


    with raises(ValueError):
        Aggregator()


def test_aggregator_without_mappings_or_aggregations_fails_to_create():
    class Aggregator(DictionaryAggregator):
        pass

    with raises(ValueError):
        Aggregator()
