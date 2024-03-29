# PyTransmuter

PyTransmuter is a Python library designed for efficient data mapping and aggregation 
from source models to target models, leveraging the power of Pydantic for data validation
and transformation. It simplifies the process of transforming data structures, making it 
an essential tool for applications requiring data normalization, transformation, and aggregation.

## Features

- **Generic Mapping**: Utilizes generic typing to map data between different model structures.
- **Flexible Aggregation**: Supports complex data aggregation strategies, including grouping, 
sorting, and custom aggregation functions.
- **Self-Inspection**: Incorporates self-inspection capabilities for resolving callables related 
to class instances.
- **Pydantic Integration**: Leverages Pydantic models for input and output validation, ensuring 
data integrity.
- **Customizable Transformations**: Allows defining custom field transformations using callable 
functions or lambdas.

## Installation

To install the library, run the following command in your terminal:

```bash
pip install py-transmuter
```

## Usage of the Mapper

### Basic Setup

Start by defining your source and target Pydantic models that represent the structure of your input and output data:

```python
from pydantic import BaseModel

class SourceModel(BaseModel):
    id: int
    temperature_celsius: float
    humidity_percentage: int

class TargetModel(BaseModel):
    id: int
    temperature_fahrenheit: float
    humidity_proportion: float
```

### Implementing a Mapper

Define a mapper by inheriting from `BaseModelMapper`, specifying how each field in the source model maps to the target model:

```python
from py_transmuter.pydantic.mapper import BaseModelMapper

class MyMapper(BaseModelMapper[TargetModel, SourceModel]):
    mapping = {
        "id": "id",
        "temperature_fahrenheit": ("temperature_celsius", lambda c: c * 9 / 5 + 32),
        "humidity_proportion": ("humidity_percentage", lambda h: h / 100.0),
    }
```

### Mapping Data

Use your mapper to transform data from the source model to the target model:

```python
source_data = SourceModel(id=1, temperature_celsius=25, humidity_percentage=50)

mapper = MyMapper()
target_data = mapper.map(source_data)

print(target_data)
# Output: TargetModel(id=1, temperature_fahrenheit=77.0, humidity_proportion=0.5)
```

### Mapping Lists of Data

`BaseModelMapper` also supports mapping lists of data from source models to target models:

```python
source_list = [
    SourceModel(id=1, temperature_celsius=25, humidity_percentage=50),
    SourceModel(id=2, temperature_celsius=20, humidity_percentage=60),
]

mapped_list = mapper.map_list(source_list)

for item in mapped_list:
    print(item)
# Output: List of TargetModel instances with mapped data
```


## Usage of the Aggregator

### Basic Setup

First, define your source and target Pydantic models:

```python
from pydantic import BaseModel

class SourceModel(BaseModel):
    id: int
    first_name: str
    last_name: str

class TargetModel(BaseModel):
    full_names: list[str]
```

### Aggregating Data

To aggregate data from a list of `SourceModel` instances into a list of `TargetModel` instances, define an aggregator class by inheriting from `BaseModelAggregator`:

```python
from py_transmuter.pydantic.aggregator import BaseModelAggregator

class MyAggregator(BaseModelAggregator[TargetModel, SourceModel]):
    mappings = {"full_names": lambda data: f"{data.first_name} {data.last_name}"}
```

### Using the Aggregator

Once your aggregator is defined, you can use it to aggregate data as follows:

```python
# Sample data
source_data = [
    SourceModel(id=1, first_name="Jane", last_name="Doe"),
    SourceModel(id=2, first_name="John", last_name="Doe"),
]

# Aggregating data
aggregator = MyAggregator()
target_data = aggregator.aggregate(source_data)

print(target_data)
# Output: [TargetModel(full_names=['Jane Doe', 'John Doe'])]
```

### Advanced Usage of the Aggregator

For more complex scenarios, `py-transmuter` allows for advanced data transformation capabilities, including grouping, sorting, and using custom functions for mappings and aggregations. Here’s an elaborate example that showcases these features.

#### Scenario

Imagine you have a dataset of measurements taken by different sensors in a scientific experiment. Each measurement includes the sensor's ID, the timestamp of the measurement, and the measured value. Your goal is to aggregate these measurements by sensor ID and day, calculate the daily average value for each sensor, and sort the results by date.

#### Source and Target Models

First, define your source model for individual measurements and a target model for the aggregated data:

```python
from pydantic import BaseModel
from datetime import datetime, date

class Measurement(BaseModel):
    sensor_id: int
    timestamp: datetime
    value: float

class DailyAverage(BaseModel):
    sensor_id: int
    date: date
    average_value: float
```

#### Aggregator Class

Next, define the aggregator class that specifies how to group measurements, how to calculate the daily averages, and how to sort the results:

```python
from py_transmuter.pydantic.aggregator import BaseModelAggregator
from statistics import mean

class MeasurementAggregator(BaseModelAggregator[DailyAverage, Measurement]):
    # Group by sensor ID and the date part of the timestamp
    group_by = (
        "sensor_id",
        lambda x: x.timestamp.date(),
    )
    
    # Define the aggregation to calculate the average value
    aggregations = {
        "sensor_id": ("sensor_id", lambda ids: ids[0]),
        "date": ("timestamp", lambda stamps: stamps[0].date()),
        "average_value": (
            "value",
            lambda values: mean(values),
        ),
    }
```

#### Executing the aggregation

With the aggregator defined, you can transform a list of `Measurement` instances into a list of `DailyAverage` instances, grouped by sensor ID and date, with the daily average value calculated for each group:

```python
# Sample data: a list of measurements
measurements = [
    Measurement(sensor_id=1, timestamp=datetime(2024, 1, 1, 12, 30), value=10),
    Measurement(sensor_id=1, timestamp=datetime(2024, 1, 1, 13, 45), value=20),
    Measurement(sensor_id=2, timestamp=datetime(2024, 1, 1, 14, 15), value=30),
    # More measurements...
]

# Instantiate the aggregator and aggregate the data
aggregator = MeasurementAggregator()
daily_averages = aggregator.aggregate(measurements)

# Output the aggregated data
print(daily_averages)
# [DailyAverage(sensor_id=1, date=date(2024, 1, 1), average_value=15), DailyAverage(sensor_id=2, date=date(2024, 1, 1), value=30), ...]
```

### Using mappings in the Aggregator

An `Aggregator` can have both `aggregation` and `mappings`; the former were explained above, but the latter serve a way simpler purpose: simply extract the value of field
for every element in the group and store it in a list; for example:

```python
class Child(BaseModel):
    parent: str
    first_name: str
    last_name: str

class Parent(BaseModel):
    name: str
    children: list[str]

class ChildParentAggregator(BaseModelAggregator[Parent, Child]):
    group_by = ('parent',)

    mappings = {"children": lambda child: f"{child.first_name} {child.last_name}"}
    aggregations = {"name": ("parent": lambda names: names[0])}
```

With this setup, we would then get:

```python
children = [
    Child(parent="Tom Smith", first_name="Paul", last_name="Smith"),
    Child(parent="Anna Lopez", first_name="Tupac", last_name="Towers"),
    Child(parent="Tom Smith", first_name="Laura", last_name="Smith")
]

parents = ChildParentAggregator().aggregate(children)
print(parents)
# [Parent(name="Tom Smith", children=["Paul Smith", "Laura Smith"]), Parent(name="Anna Lopez", children=["Tupac Towers"])]
```

## Using self inspection

There are many scenarios in which we need values that are known only in runtime and not when declaring the mappings and aggregations for our `Mapper` or `Aggregator` class;
for this, both these classes are capable of identifying methods that "belong to them" and it is possible to add them to the static definitions of `mappings` and `aggregations`,
and can later access class or instance attributes in runtime.

An example of this would be:

```python
class A(BaseModel):
    id: int

class B(BaseModel):
    id: str

class ABMapper(BaseModelMapper[B, A]):
    def map_id(self, data: A) -> str:
        return str(data.id * self.context["factor"])

    mapping = {"id": map_id}

assert ABMapper(context={"factor": 2}).map(A(id=1)) == B(id="2")
```

The `context` attribute is included by default in both the `Mapper` and the `Aggregator`, but you could define the class as you wish (with any attributes you'd want) and 
access them in the moment they are needed:

```python
class Mapper(BaseModelMapper[B, A]):
    attribute: Any

    def __init__(self, attribute: Any, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.attribute = attribute
```
