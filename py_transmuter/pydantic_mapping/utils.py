import inspect
from types import UnionType
from typing import Optional, Union, get_type_hints
from pydantic import BaseModel


def get_required_fields(model_cls: type[BaseModel]) -> list[str]:
    required_fields = list()

    type_hints = get_type_hints(model_cls)
    for field, field_type in type_hints.items():
        # For Python 3.10 and later: Check if field_type is UnionType and contains None
        if isinstance(field_type, UnionType):
            if type(None) not in field_type.__args__:
                required_fields.append(field)
        # For earlier versions: Check if field_type is a Union and if NoneType is one of the Union types
        elif getattr(field_type, "__origin__", None) is Union:
            if type(None) not in getattr(field_type, "__args__", ()):
                required_fields.append(field)
        # Check if the type hint is explicitly Optional but not a Union (for completeness)
        elif not (
            inspect.isclass(field_type) and issubclass(field_type, type(Optional))
        ):
            required_fields.append(field)

    return required_fields
