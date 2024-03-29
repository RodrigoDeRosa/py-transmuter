from datetime import date, datetime, timedelta
from statistics import mean
from pydantic import BaseModel
from pytest import raises
from py_transmuter.pydantic.aggregator import BaseModelAggregator


def test_aggregator_field_direct_mappings():
    class A(BaseModel):
        id: int

    class B(BaseModel):
        ids: list[int]

    class ABAggregator(BaseModelAggregator[B, A]):
        mappings = {"ids": "id"}

    assert ABAggregator().aggregate([A(id=1), A(id=2), A(id=3)]) == [B(ids=[1, 2, 3])]


def test_aggregator_tuple_direct_mappings():
    class A(BaseModel):
        id: int

    class B(BaseModel):
        ids: list[str]

    class ABAggregator(BaseModelAggregator[B, A]):
        mappings = {"ids": ("id", lambda id: str(id))}

    assert ABAggregator().aggregate([A(id=1), A(id=2), A(id=3)]) == [
        B(ids=["1", "2", "3"])
    ]


def test_aggregator_model_callable_direct_mappings():
    class A(BaseModel):
        first_name: str
        last_name: str

    class B(BaseModel):
        full_names: list[str]

    def make_full_name(data: A) -> str:
        return f"{data.first_name} {data.last_name}"

    class ABAggregator(BaseModelAggregator[B, A]):
        mappings = {"full_names": make_full_name}

    assert ABAggregator().aggregate(
        [
            A(first_name="Rodrigo", last_name="De Rosa"),
            A(first_name="Marcelo", last_name="Gallardo"),
            A(first_name="Super", last_name="Man"),
        ]
    ) == [B(full_names=["Rodrigo De Rosa", "Marcelo Gallardo", "Super Man"])]


def test_aggregator_field_direct_mappings_with_grouping():
    class Child(BaseModel):
        parent: str

        first_name: str
        last_name: str

    class Parent(BaseModel):
        children_names: list[str]

    def make_full_name(data: Child) -> str:
        return f"{data.first_name} {data.last_name}"

    class ChildParentAggregator(BaseModelAggregator[Parent, Child]):
        group_by = ("parent",)

        mappings = {"children_names": make_full_name}

    assert ChildParentAggregator().aggregate(
        [
            Child(parent="Patricio", first_name="Rodrigo", last_name="De Rosa"),
            Child(parent="God Himself", first_name="Marcelo", last_name="Gallardo"),
            Child(parent="God Himself", first_name="Super", last_name="Man"),
        ]
    ) == [
        Parent(children_names=["Rodrigo De Rosa"]),
        Parent(children_names=["Marcelo Gallardo", "Super Man"]),
    ]


def test_aggregator_group_by_with_extractor():
    class Child(BaseModel):
        parent: str

        first_name: str
        last_name: str

    class Parent(BaseModel):
        children_names: list[str]

    class ChildParentAggregator(BaseModelAggregator[Parent, Child]):
        @staticmethod
        def parent_full_name(data: Child) -> str:
            return f"{data.parent} {data.last_name}"

        group_by = (parent_full_name,)

        mappings = {"children_names": "first_name"}

    assert ChildParentAggregator().aggregate(
        [
            Child(parent="Tom", first_name="Paul", last_name="Doe"),
            Child(parent="Tom", first_name="George", last_name="Stones"),
            Child(parent="Tom", first_name="Carl", last_name="Doe"),
        ]
    ) == [Parent(children_names=["Paul", "Carl"]), Parent(children_names=["George"])]


def test_aggregator_with_grouping_by_field_and_extractor():
    class Measurement(BaseModel):
        machine: str
        timestamp: datetime
        value: float

    class DayValues(BaseModel):
        intervals: list[datetime]
        values: list[float]

    class DailyMeasurementAggregator(BaseModelAggregator[DayValues, Measurement]):
        @staticmethod
        def timestamp_date(data: Measurement) -> date:
            return data.timestamp.date()

        group_by = ("machine", timestamp_date)

        mappings = {"intervals": "timestamp", "values": "value"}

    aggregator = DailyMeasurementAggregator()

    day_values = aggregator.aggregate(
        [
            Measurement(
                machine="A",
                timestamp=datetime(2024, 2, 16) + timedelta(hours=12 * i),
                value=100,
            )
            for i in range(4)
        ]
    )

    assert day_values == [
        DayValues(
            intervals=[datetime(2024, 2, 16), datetime(2024, 2, 16, 12)],
            values=[100, 100],
        ),
        DayValues(
            intervals=[datetime(2024, 2, 17), datetime(2024, 2, 17, 12)],
            values=[100, 100],
        ),
    ]


def test_aggregator_with_sort_by_field():
    class A(BaseModel):
        id: int

    class B(BaseModel):
        ids: list[int]

    class ABAggregator(BaseModelAggregator[B, A]):
        sort_by = ("id",)

        mappings = {"ids": "id"}

    assert ABAggregator().aggregate([A(id=3), A(id=1), A(id=2)]) == [B(ids=[1, 2, 3])]


def test_aggregator_with_sort_by_extractor():
    class Child(BaseModel):
        first_name: str
        last_name: str

    class Parent(BaseModel):
        children_names: list[str]

    def make_full_name(data: Child) -> str:
        return f"{data.first_name} {data.last_name}"

    class ChildParentAggregator(BaseModelAggregator[Parent, Child]):
        sort_by = (make_full_name,)

        mappings = {"children_names": make_full_name}

    assert ChildParentAggregator().aggregate(
        [
            Child(first_name="Paul", last_name="Smith"),
            Child(first_name="Paul", last_name="Holmes"),
        ]
    ) == [Parent(children_names=["Paul Holmes", "Paul Smith"])]


def test_aggregator_with_sort_by_extractor_and_field():
    class Child(BaseModel):
        first_name: str
        last_name: str

        age: int

    class Parent(BaseModel):
        children_names: list[str]
        ages: list[int]

    def make_full_name(data: Child) -> str:
        return f"{data.first_name} {data.last_name}"

    class ChildParentAggregator(BaseModelAggregator[Parent, Child]):
        sort_by = (make_full_name, "age")

        mappings = {"children_names": make_full_name, "ages": "age"}

    assert ChildParentAggregator().aggregate(
        [
            Child(first_name="Paul", last_name="Smith", age=17),
            Child(first_name="Paul", last_name="Holmes", age=23),
            Child(first_name="Paul", last_name="Smith", age=5),
        ]
    ) == [
        Parent(
            children_names=["Paul Holmes", "Paul Smith", "Paul Smith"], ages=[23, 5, 17]
        )
    ]


def test_aggregator_with_field_mapper_aggregations():
    class Vertical(BaseModel):
        value: float

    class Horizontal(BaseModel):
        values: list[float]

    def aggregate_values(values: list[float]) -> float:
        average = sum(values) / len(values)
        return [value / average for value in values]

    class VerticalToHorizontalAggregator(BaseModelAggregator[Horizontal, Vertical]):
        aggregations = {
            "values": ("value", aggregate_values),
        }

    assert VerticalToHorizontalAggregator().aggregate(
        [Vertical(value=6), Vertical(value=4), Vertical(value=2)]
    ) == [Horizontal(values=[1.5, 1, 0.5])]

    
def test_aggregator_with_extractor_aggregations():
    class Coordinates(BaseModel):
        latitude: float
        longitude: float

    class Turbine(BaseModel):
        latitude: float
        longitude: float

    class Park(BaseModel):
        location: Coordinates

    def average_coordinates(turbines: list[Turbine]) -> Coordinates:
        return Coordinates(
            latitude=mean(turbine.latitude for turbine in turbines),
            longitude=mean(turbine.longitude for turbine in turbines),
        )

    class TurbineParkAggregator(BaseModelAggregator[Park, Turbine]):
        aggregations = {
            "location": average_coordinates,
        }

    assert TurbineParkAggregator().aggregate(
        [
            Turbine(latitude=10, longitude=5), 
            Turbine(latitude=15, longitude=10),
            Turbine(latitude=20, longitude=0),
        ]
    ) == [Park(location=Coordinates(latitude=15, longitude=5))]


def test_aggregator_with_mappings_and_aggregations():
    class A(BaseModel):
        parent_id: int
        value: float

    class B(BaseModel):
        id: int
        values: list[float]

    class ABAggregator(BaseModelAggregator[B, A]):
        group_by = ("parent_id",)

        mappings = {"values": "value"}
        aggregations = {"id": ("parent_id", lambda ids: ids[0])}

    assert ABAggregator().aggregate(
        [A(parent_id=1, value=10), A(parent_id=1, value=20), A(parent_id=2, value=30)]
    ) == [B(id=1, values=[10, 20]), B(id=2, values=[30])]


def test_aggregator_with_overlapping_mappings_and_aggregations_fails_to_create():
    class A(BaseModel):
        parent_id: int

    class B(BaseModel):
        id: int

    class ABAggregator(BaseModelAggregator[B, A]):
        mappings = {"id": "parent_id"}
        aggregations = {"id": ("parent_id", lambda ids: ids[0])}


    with raises(ValueError):
        ABAggregator()


def test_aggregator_without_mappings_or_aggregations():
    class A(BaseModel):
        id: int

    class B(BaseModel):
        id: int

    class ABAggregator(BaseModelAggregator[B, A]):
        pass

    with raises(ValueError):
        ABAggregator()


def test_aggregator_missing_required_fields():
    class A(BaseModel):
        id: int
        value: float

    class B(BaseModel):
        id: int
        values: list[float]

    class ABAggregator(BaseModelAggregator[B, A]):
        aggregations = {"values": "value"}

    with raises(ValueError):
        ABAggregator()


def test_aggregator_with_extra_fields():
    class A(BaseModel):
        id: int
        value: float

    class B(BaseModel):
        id: int

    class ABAggregator(BaseModelAggregator[B, A]):
        aggregations = {"id": "id", "values": "value"}

    with raises(ValueError):
        ABAggregator()