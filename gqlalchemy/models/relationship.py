from __future__ import annotations

import re
from typing import TYPE_CHECKING, ClassVar, Dict, Self, Tuple, dataclass_transform

from pydantic.fields import PrivateAttr as PydanticPrivateAttr
from pydantic._internal._model_construction import ModelMetaclass

from gqlalchemy.exceptions import GQLAlchemyError
from gqlalchemy.models.graph_object import GraphObject, Field, FieldInfo


@dataclass_transform(kw_only_default=False, field_specifiers=(FieldInfo, PydanticPrivateAttr))
class RelationshipMetaclass(ModelMetaclass):

    __relationship_registry: ClassVar[Dict[str, type[Relationship]]] = {}
    """Maps relationship unique type to its class"""

    def __new__(mcs, name, bases, namespace, **kwargs):
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        if hasattr(cls, "type") and isinstance(cls.type, str):
            mcs.__relationship_registry[cls.type] = cls
        return cls

    @classmethod
    def get_relationship_class_by_type(mcs, type: str) -> type[Relationship]:
        return mcs.__relationship_registry[type]


if TYPE_CHECKING:
    from gqlalchemy.vendors.database_client import DatabaseClient


class Relationship(GraphObject, metaclass=RelationshipMetaclass):

    type: ClassVar[str] = "Relationship"

    _id: int | None = PydanticPrivateAttr()

    start_node_id: int = Field(None, exclude=True, init=True)
    end_node_id: int = Field(None, exclude=True, init=True)

    @classmethod
    def __init_subclass__(
        cls,
        type: str | None = None,
        **kwargs,
    ):
        super().__init_subclass__(**kwargs)

        if type is None:
            snake_case = re.sub(r"(?<!^)(?=[A-Z])", "_", cls.__name__)
            type = snake_case.upper()
        cls.type = type

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs):
        super().__pydantic_init_subclass__(**kwargs)

    def __init__(self, **data):
        super().__init__(**data)

        self._id = None

        # TODO: relax this
        if self.start_node_id is None:
            raise GQLAlchemyError({"message": "Start node must have an id"})
        if self.end_node_id is None:
            raise GQLAlchemyError({"message": "End node must have an id"})

    @property
    def _type(self) -> str:
        return type(self).type

    @property
    def _start_node_id(self) -> int:
        return self.start_node_id

    @property
    def _end_node_id(self) -> int:
        return self.end_node_id

    @property
    def _nodes(self) -> Tuple[int, int]:
        return (self.start_node_id, self.end_node_id)

    def __str__(self) -> str:
        return "".join(
            (
                f"<{self._type}",
                f" id={self._id}",
                f" start_node_id={self.start_node_id}",
                f" end_node_id={self.end_node_id}",
                f" properties={self._properties}",
                ">",
            )
        )

    def save(self, db: DatabaseClient) -> Self:
        """Saves a relationship to Memgraph.
        If relationship._id is not None it finds the relationship in Memgraph
        and updates it's properties with the values in `relationship`.
        If relationship._id is None, it creates a new relationship.
        If you want to set a relationship._id instead of creating a new
        relationship, use `load_relationship` first.
        """
        db.save_relationship(self)
        return self

    def load(self, db: DatabaseClient) -> Self:
        """Returns a relationship loaded from Memgraph.
        If the relationship._id is not None it fetches the relationship from
        Memgraph that has the same internal id.
        Otherwise it returns the relationship whose relationship._start_node_id
        and relationship._end_node_id and all relationship properties that
        are not None match the relationship in Memgraph.
        If there is no relationship like that in Memgraph, or if there are
        multiple relationships like that in Memgraph, throws GQLAlchemyError.
        """
        relationship = db.load_relationship(self)
        if relationship is None:
            raise GQLAlchemyError({"message": "No relationship or multiple could be found"})
        for field_name in self.model_fields.keys():
            setattr(self, field_name, getattr(relationship, field_name))
        self._id = relationship._id
        return self

    def get_or_create(self, db: DatabaseClient) -> Tuple[Self, bool]:
        """Return the relationship and a flag for whether it was created in the database.
        Args:
            db: The database instance to operate on.
        Returns:
            A tuple with the first component being the created graph relationship,
            and the second being a boolean that is True if the relationship
            was created in the database, and False if it was loaded instead.
        """
        try:
            return self.load(db=db), False
        except GQLAlchemyError:
            self.save(db=db)
            return self, True
