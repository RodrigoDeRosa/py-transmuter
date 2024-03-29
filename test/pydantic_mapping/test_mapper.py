from datetime import datetime
from pydantic import BaseModel
from pytest import raises
from py_transmuter.pydantic_mapping.mapper import BaseModelMapper


def test_map_with_field_name():
    class A(BaseModel):
        id: int

    class B(BaseModel):
        id: int

    class ABMapper(BaseModelMapper[B, A]):
        mapping = {"id": "id"}

    assert ABMapper().map(A(id=1)) == B(id=1)


def test_map_with_lambda():
    class A(BaseModel):
        id: int

    class B(BaseModel):
        id: str

    class ABMapper(BaseModelMapper[B, A]):
        mapping = {"id": lambda data: str(data.id)}

    assert ABMapper().map(A(id=1)) == B(id="1")


def test_map_with_function():
    class A(BaseModel):
        id: int

    class B(BaseModel):
        id: str

    def map_id(data: A) -> str:
        return str(data.id * 10)

    class ABMapper(BaseModelMapper[B, A]):
        mapping = {"id": map_id}

    assert ABMapper().map(A(id=1)) == B(id="10")


def test_map_with_context_and_self_inspection():
    class A(BaseModel):
        id: int

    class B(BaseModel):
        id: str

    class ABMapper(BaseModelMapper[B, A]):
        def map_id(self, data: A) -> str:
            return str(data.id * self.context["factor"])

        mapping = {"id": map_id}

    assert ABMapper(context={"factor": 2}).map(A(id=1)) == B(id="2")


def test_map_with_class_method():
    class A(BaseModel):
        id: int

    class B(BaseModel):
        id: int

    class ABMapper(BaseModelMapper[B, A]):
        FACTOR = 10

        @classmethod
        def map_id(cls, data: A) -> int:
            return data.id * cls.FACTOR

        mapping = {"id": map_id}

    assert ABMapper().map(A(id=1)) == B(id=10)


def test_map_with_tuple_accessor():
    class A(BaseModel):
        id: int

    class B(BaseModel):
        id: int

    class ABMapper(BaseModelMapper[B, A]):
        mapping = {"id": ("id", lambda value: value * 10)}

    assert ABMapper().map(A(id=1)) == B(id=10)


def test_map_list():
    class A(BaseModel):
        id: int

    class B(BaseModel):
        id: int

    class ABMapper(BaseModelMapper[B, A]):
        mapping = {"id": "id"}

    assert ABMapper().map_list([A(id=1), A(id=2)]) == [B(id=1), B(id=2)]


def test_map_without_optional_fields():
    class A(BaseModel):
        id: int

    class B(BaseModel):
        id: int
        name: str | None = None

    class ABMapper(BaseModelMapper[B, A]):
        mapping = {"id": "id"}

    assert ABMapper().map(A(id=1)) == B(id=1)


def test_map_missing_required_field_fails():
    class A(BaseModel):
        id: int

    class B(BaseModel):
        id: int
        name: str

    class ABMapper(BaseModelMapper[B, A]):
        mapping = {"id": "id"}

    with raises(ValueError):
        ABMapper()


def test_map_extra_field_fails():
    class A(BaseModel):
        id: int

    class B(BaseModel):
        id: int

    class ABMapper(BaseModelMapper[B, A]):
        mapping = {"id": "id", "name": "name"}

    with raises(ValueError):
        ABMapper()


def test_map_with_optional_field():
    class A(BaseModel):
        id: int

    class B(BaseModel):
        id: int
        name: str | None = None

    class ABMapper(BaseModelMapper[B, A]):
        mapping = {"id": "id", "name": lambda data: "Rodrigo"}

    assert ABMapper().map(A(id=1)) == B(id=1, name="Rodrigo")


def test_map_complex_models():
    class WeatherDataBackend(BaseModel):
        temperature_fahrenheit: float
        humidity_proportion: float
        timestamp: datetime
        
        country: str | None = None

    class WeatherDataUI(BaseModel):
        temperature_celsius: float
        humidity_percentage: int
        date: str
        time: str

        language: str

    class WeatherDataMapper(BaseModelMapper[WeatherDataBackend, WeatherDataUI]):
        @staticmethod
        def celsius_to_fahrenheit(celsius: float) -> float:
            return (celsius * 9 / 5) + 32

        @staticmethod
        def parse_date_time_to_object(data: WeatherDataUI) -> datetime:
            return datetime.strptime(
                f"{data.date} {data.time}", "%Y-%m-%d %H:%M"
            )

        mapping = {
            "temperature_fahrenheit": ("temperature_celsius", celsius_to_fahrenheit),
            "humidity_proportion": ("humidity_percentage", lambda humidity: float(humidity) / 100),
            "timestamp": parse_date_time_to_object,
        }

    to_map = WeatherDataUI(
        temperature_celsius=10,
        humidity_percentage=50,
        date="2021-01-01", 
        time="12:00",
        language="en",
    )

    mapped = WeatherDataMapper().map(to_map)
    assert mapped == WeatherDataBackend(
        temperature_fahrenheit=50.0,
        humidity_proportion=0.5,
        timestamp=datetime(2021, 1, 1, 12)
    )
