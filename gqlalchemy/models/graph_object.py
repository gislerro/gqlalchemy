from dataclasses import dataclass
from datetime import datetime, timedelta, date, time
from enum import Enum
from functools import lru_cache
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Concatenate,
    Optional,
    ParamSpec,
    TypeVar,
    Union,
    cast,
    ClassVar,
    Dict,
)


from pydantic import BaseModel, Field as PydanticField
from pydantic.fields import FieldInfo as PydanticFieldInfo

from gqlalchemy.exceptions import GQLAlchemyError


@dataclass
class GQLConfig:
    index: bool | None = False
    # could be derived from FieldInfo.required?
    exists: bool | None = False
    unique: bool | None = False

    if TYPE_CHECKING:
        from gqlalchemy.vendors.database_client import DatabaseClient

        db: DatabaseClient | None = None
    else:
        db: Any = None


@dataclass
class GQLMetadata:
    config: GQLConfig
    on_disk: bool | None = False


# Adapted from: https://stackoverflow.com/questions/71968447/python-typing-copy-kwargs-from-one-function-to-another
# also refer to discussion here: https://discuss.python.org/t/taking-the-argument-signature-from-a-different-function/42618/20
#
# FIXME: ...in the future - It's currently impossible to slot in 'MyMetadata' as a named kwarg:
# https://peps.python.org/pep-0612/#concatenating-keyword-parameters
#
P = ParamSpec("P")  # param spec of wrapper
T = TypeVar("T")  # return type of wrapped function


def wrap(_: Callable[P, Any]) -> Callable[[Callable[..., T]], Callable[Concatenate[GQLConfig | None, P], T]]:
    """Wrap a `Converter` `__init__` in a type-safe way."""

    def impl(fun: Callable[..., T]) -> Callable[Concatenate[GQLConfig | None, P], T]:
        return cast(Callable[Concatenate[Optional[GQLConfig], P], T], fun)

    return impl


@wrap(PydanticField)
def Field(config: GQLConfig | None, *args, **kwargs):
    field = PydanticField(*args, **kwargs)

    if config is None:
        config = GQLConfig()

    gql = GQLMetadata(config=config)

    field.metadata.append(gql)
    return field


class DatetimeKeywords(Enum):
    DURATION = "duration"
    LOCALTIME = "localTime"
    LOCALDATETIME = "localDateTime"
    DATE = "date"


datetimeKwMapping = {
    timedelta: DatetimeKeywords.DURATION.value,
    time: DatetimeKeywords.LOCALTIME.value,
    datetime: DatetimeKeywords.LOCALDATETIME.value,
    date: DatetimeKeywords.DATE.value,
}


def _format_timedelta(duration: timedelta) -> str:
    days = int(duration.total_seconds() // 86400)
    remainder_sec = duration.total_seconds() - days * 86400
    hours = int(remainder_sec // 3600)
    remainder_sec -= hours * 3600
    minutes = int(remainder_sec // 60)
    remainder_sec -= minutes * 60

    return f"P{days}DT{hours}H{minutes}M{remainder_sec}S"


class FieldInfo(PydanticFieldInfo):
    metadata: list[Any | GQLMetadata]


class GraphObject(BaseModel):

    if TYPE_CHECKING:
        # valid since FieldInfo is a subtype of pydantic's FieldInfo
        model_fields: ClassVar[Dict[str, FieldInfo]]  # type: ignore

    @classmethod
    def get_metadata_from_field(cls, field_name: str) -> GQLMetadata:
        for metadata in cls.model_fields[field_name].metadata:
            if isinstance(metadata, GQLMetadata):
                return metadata

        # create a new metadata object if it doesn't exist, i.e when pydantic's Field is used
        default_metadata = GQLMetadata(config=GQLConfig())
        cls.model_fields[field_name].metadata.append(default_metadata)
        return default_metadata

    @classmethod
    @lru_cache
    def get_metadata(cls) -> Dict[str, GQLMetadata]:
        # TODO: check if mutating gql metadata invalidates the cache of this function
        metadata = {}
        for field_name in cls.model_fields.keys():
            field_metadata = cls.get_metadata_from_field(field_name)
            metadata[field_name] = field_metadata
        return metadata

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        try:
            cls.get_metadata()
        except GQLAlchemyError:
            raise GQLAlchemyError(
                f"""Class {cls.__name__} is missing GqlFieldMetadata, make sure you use the Field exported from gqlalchemy in your models.\n\nfrom gqlalchemy import Field"""
            )

    @property
    def _properties(self) -> Dict[str, Any]:
        return self.model_dump()

    def escape_value(
        self, value: Union[None, bool, int, float, str, list, dict, datetime, timedelta, date, time]
    ) -> str:
        if value is None:
            "Null"
        elif isinstance(value, bool):
            return repr(value)
        elif isinstance(value, int):
            return repr(value)
        elif isinstance(value, float):
            return repr(value)
        elif isinstance(value, str):
            return repr(value) if value.isprintable() else rf"'{value}'"
        elif isinstance(value, list):
            return "[" + ", ".join(self.escape_value(val) for val in value) + "]"
        elif isinstance(value, dict):
            return "{" + ", ".join(f"{key}: {self.escape_value(val)}" for key, val in value.items()) + "}"

        if isinstance(value, datetime):
            if value.tzinfo is not None:
                tz_offset = value.strftime("%z")
                tz_name = value.tzinfo.tzname
                return f"datetime('{value.strftime('%Y-%m-%dT%H:%M:%S')}{tz_offset}[{tz_name}]')"
            keyword = datetimeKwMapping[datetime]
            formatted_value = value.isoformat()
            return f"{keyword}('{formatted_value}')"
        elif isinstance(value, timedelta):
            formatted_value = _format_timedelta(value)
            keyword = datetimeKwMapping[timedelta]
            return f"{keyword}('{formatted_value}')"
        elif isinstance(value, (time, date)):
            formatted_value = value.isoformat()
            keyword = datetimeKwMapping[type(value)]
            return f"{keyword}('{formatted_value}')"
        else:
            raise GQLAlchemyError(
                f"Unsupported value data type: {type(value)}."
                + " Memgraph supports the following data types:"
                + " None, bool, int, float, str, list, dict, datetime."
            )

    def _get_cypher_field_assignment_block(self, variable_name: str, operator: str) -> str:
        """Creates a cypher field assignment block joined using the `operator`
        argument.
        Example:
            self = {"name": "John", "age": 34}
            variable_name = "user"
            operator = " AND "

            returns:
                "user.name = 'John' AND user.age = 34"
        """
        cypher_fields = []
        for field_name, value in self._properties.items():
            if value is not None:
                cypher_fields.append(f"{variable_name}.{field_name} = {self.escape_value(value)}")

        return " " + operator.join(cypher_fields) + " "

    def _get_cypher_fields_or_block(self, variable_name: str) -> str:
        """Returns a cypher field assignment block separated by an OR
        statement.
        """
        return self._get_cypher_field_assignment_block(variable_name, " OR ")

    def _get_cypher_fields_and_block(self, variable_name: str) -> str:
        """Returns a cypher field assignment block separated by an AND
        statement.
        """
        return self._get_cypher_field_assignment_block(variable_name, " AND ")

    def _get_cypher_fields_xor_block(self, variable_name: str) -> str:
        """Returns a cypher field assignment block separated by an XOR
        statement.
        """
        return self._get_cypher_field_assignment_block(variable_name, " XOR ")

    # TODO: add NOT

    def _get_cypher_set_properties(self, variable_name: str) -> str:
        """Returns a cypher set properties block."""
        cypher_set_properties = []

        for field_name, value in self._properties.items():
            gql_metadata = self.get_metadata_from_field(field_name)
            if value is not None and not gql_metadata.on_disk:
                cypher_set_properties.append(f" SET {variable_name}.{field_name} = {self.escape_value(value)}")

        return " " + " ".join(cypher_set_properties) + " "

    def __str__(self) -> str:
        return "<GraphObject>"

    def __repr__(self) -> str:
        return str(self)
