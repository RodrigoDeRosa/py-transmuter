from datetime import date
from typing import Any

from pytest import raises
from py_transmuter.dictionaries.mapper import DictionaryMapper


def test_map_with_field_name():
    class Mapper(DictionaryMapper):
        mapping = {"id": "id"}

    assert Mapper().map({"id": 1}) == {"id": 1}


def test_map_with_lambda():
    class Mapper(DictionaryMapper):
        mapping = {"id": lambda data: str(data["id"])}

    assert Mapper().map({"id": 1}) == {"id": "1"}


def test_map_with_function():
    def map_id(data: dict[Any, Any]) -> str:
        return str(data["id"] * 10)

    class Mapper(DictionaryMapper):
        mapping = {"id": map_id}

    assert Mapper().map({"id": 1}) == {"id": "10"}


def test_map_with_field_function():
    class Mapper(DictionaryMapper):
        mapping = {"id": ("id", str)}

    assert Mapper().map({"id": 1}) == {"id": "1"}


def test_map_with_context_and_self_inspection():
    class Mapper(DictionaryMapper):
        def map_id(self, data: dict[Any, Any]) -> str:
            return str(data["id"] * self.context["factor"])

        mapping = {"id": map_id}

    assert Mapper(context={"factor": 2}).map({"id": 1}) == {"id": "2"}


def test_map_with_class_method():
    class Mapper(DictionaryMapper):
        FACTOR = 10

        @classmethod
        def map_id(cls, data: dict[Any, Any]) -> int:
            return data["id"] * cls.FACTOR

        mapping = {"id": map_id}

    assert Mapper().map({"id": 1}) == {"id": 10}


def test_map_with_static_method():
    class Mapper(DictionaryMapper):
        FACTOR = 10

        @staticmethod
        def map_id(data: dict[Any, Any]) -> int:
            return data["id"] * 10

        mapping = {"id": map_id}

    assert Mapper().map({"id": 1}) == {"id": 10}


def test_map_list():
    class Mapper(DictionaryMapper):
        mapping = {"id": "id"}

    assert Mapper().map_list([{"id": 1}, {"id": 2}, {"id": 3}]) == [
        {"id": 1},
        {"id": 2},
        {"id": 3},
    ]


def test_create_invalid_mapper():
    class Mapper(DictionaryMapper):
        mapping = None

    with raises(ValueError):
        Mapper()


def test_map_with_any_type_keys():
    class Mapper(DictionaryMapper):
        mapping = {
            "id": 1,
            ("march", 14): date(2021, 3, 14),
        }

    assert Mapper().map({1: "an_id", date(2021, 3, 14): "Tomorrow is Pi day!"}) == {
        "id": "an_id",
        ("march", 14): "Tomorrow is Pi day!",
    }
