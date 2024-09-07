from __future__ import annotations

from collections import defaultdict
from functools import lru_cache
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Iterable, Self, Set, Tuple, dataclass_transform

from pydantic.fields import PrivateAttr as PydanticPrivateAttr
from pydantic._internal._model_construction import ModelMetaclass

from gqlalchemy.exceptions import (
    GQLAlchemyDatabaseMissingInNodeClassError,
    GQLAlchemyError,
)
from gqlalchemy.models.graph_object import GraphObject, FieldInfo

from gqlalchemy.models.constraints import Index, MemgraphConstraintUnique, MemgraphIndex, MemgraphConstraintExists


@dataclass_transform(kw_only_default=False, field_specifiers=(FieldInfo, PydanticPrivateAttr))
class NodeMetaclass(ModelMetaclass):

    __node_registry: ClassVar[Dict[str, type[Node]]] = {}
    """Maps node label to its class"""

    def __new__(mcs, name, bases, namespace, **kwargs):
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        if hasattr(cls, "label") and isinstance(cls.label, str):
            mcs.__node_registry[cls.label] = cls
        return cls

    @classmethod
    @lru_cache
    def get_node_class_by_labels(mcs, labels: Set[str]) -> type[Node]:
        classes = [mcs.__node_registry[label] for label in labels]
        counts: Dict[str, int] = defaultdict(int)
        for class1 in classes:
            counts[class1.label] += sum(issubclass(class1, class2) for class2 in classes)
        label = max(counts, key=lambda k: counts[k])
        return mcs.__node_registry[label]


if TYPE_CHECKING:
    from gqlalchemy.vendors.database_client import DatabaseClient


class Node(GraphObject, metaclass=NodeMetaclass):

    label: ClassVar[str] = "Node"
    labels: ClassVar[Set[str]] = {"Node"}
    # whether the node label should be indexed
    index: ClassVar[bool] = False

    # whether the node is an 'opaque' node
    # subclasses will not inherit the labels of the parent class
    opaque: ClassVar[bool] = False
    if TYPE_CHECKING:
        db: ClassVar[DatabaseClient | None] = None
    else:
        db: ClassVar[Any | None] = None

    # memgraph internal id
    _id: int | None = PydanticPrivateAttr()

    @classmethod
    def __init_subclass__(
        cls,
        label: str | None = None,
        labels: Set[str] | None = None,
        index: bool | None = None,
        opaque: bool = False,
        db: DatabaseClient | None = None,
        **kwargs,
    ):
        super().__init_subclass__(**kwargs)

        cls.label = label or cls.__name__
        if labels is None:

            def get_base_labels(bases: Iterable[type[Any]]) -> Set[str]:
                labels: Set[str] = set()
                for base in bases:
                    if issubclass(base, Node) and base is not Node and base.labels and not base.opaque:
                        labels = labels.union(base.labels)
                return labels

            base_labels = get_base_labels(cls.__bases__)
            cls.labels = base_labels.union({cls.label})
        else:
            cls.labels = labels.union({cls.label})

        cls.index = index or False
        cls.opaque = opaque
        cls.db = db or cls.db  # inherit db from parent class

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs):
        super().__pydantic_init_subclass__(**kwargs)

        if cls.index:
            if cls.db is None:
                raise GQLAlchemyDatabaseMissingInNodeClassError(cls=cls)
            index = Index(cls.label)
            cls.db.create_index(index)

        # Index construction on model field
        for field_name, metadata in cls.get_metadata().items():
            db = metadata.config.db or cls.db

            if metadata.config.index:
                index = MemgraphIndex(cls.label, field_name)
                if db is None:
                    raise GQLAlchemyDatabaseMissingInNodeClassError(cls=cls)
                db.create_index(index)
            if metadata.config.exists:
                exists = MemgraphConstraintExists(cls.label, field_name)
                if db is None:
                    raise GQLAlchemyDatabaseMissingInNodeClassError(cls=cls)
                db.create_constraint(exists)
            if metadata.config.unique:
                unique = MemgraphConstraintUnique(cls.label, field_name)
                if db is None:
                    raise GQLAlchemyDatabaseMissingInNodeClassError(cls=cls)
                db.create_constraint(unique)

    def __init__(self, **data):
        super().__init__(**data)
        self._id = None

    # For compatibility with the old implementation
    @property
    def _label(self) -> str:
        return type(self).label

    @property
    def _labels(self) -> str:
        return ":".join(sorted(type(self).labels))

    def __str__(self) -> str:
        return "".join(
            (
                f"<{self._label}",
                f" id={self._id}",
                f" labels={self._labels}",
                f" properties={self._properties}",
                ">",
            )
        )

    def _get_cypher_unique_fields_or_block(self, variable_name: str) -> str:
        """Get's a cypher assignment block using the unique fields."""
        cypher_unique_fields = []
        for field_name, metadata in type(self).get_metadata().items():
            if metadata.config.unique:
                value = getattr(self, field_name)
                if value is not None:
                    cypher_unique_fields.append(f"{variable_name}.{field_name} = {self.escape_value(value)}")

        return " " + " OR ".join(cypher_unique_fields) + " "

    def has_unique_fields(self) -> bool:
        """Returns True if the Node has any unique fields."""
        for field_name, metadata in type(self).get_metadata().items():
            if metadata.config.unique:
                if getattr(self, field_name) is not None:
                    return True
        return False

    def save(self, db: DatabaseClient) -> Self:
        """Saves node to Memgraph.
        If the node._id is not None it fetches the node with the same id from
        Memgraph and updates it's fields.
        If the node has unique fields it fetches the nodes with the same unique
        fields from Memgraph and updates it's fields.
        Otherwise it creates a new node with the same properties.
        Null properties are ignored.
        """
        db.save_node(self)
        return self

    def load(self, db: DatabaseClient) -> Self:
        """Loads a node from Memgraph.
        If the node._id is not None it fetches the node from Memgraph with that
        internal id.
        If the node has unique fields it fetches the node from Memgraph with
        those unique fields set.
        Otherwise it tries to find any node in Memgraph that has all properties
        set to exactly the same values.
        If no node is found or no properties are set it raises a GQLAlchemyError.
        """
        node = db.load_node(self)
        if node is None:
            raise GQLAlchemyError({"message": "No node found during loading"})
        for field_name in self.model_fields.keys():
            setattr(self, field_name, getattr(node, field_name))
        self._id = node._id
        return self

    def get_or_create(self, db: DatabaseClient) -> Tuple[Self, bool]:
        """Return the node and a flag for whether it was created in the database.

        Args:
            db: The database instance to operate on.

        Returns:
            A tuple with the first component being the created graph node,
            and the second being a boolean that is True if the node
            was created in the database, and False if it was loaded instead.
        """
        try:
            return self.load(db=db), False
        except GQLAlchemyError:
            self.save(db=db)
            return self, True
