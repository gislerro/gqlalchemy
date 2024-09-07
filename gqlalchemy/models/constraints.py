from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Tuple, Union


@dataclass(frozen=True, eq=True)
class Index(ABC):
    label: str
    property: str | None = None

    def to_cypher(self) -> str:
        return f":{self.label}{f'({self.property})' if self.property else ''}"


@dataclass(frozen=True, eq=True)
class MemgraphIndex(Index):
    pass


@dataclass(frozen=True, eq=True)
class Neo4jIndex(Index):
    type: str | None = None
    uniqueness: str | None = None


@dataclass(frozen=True, eq=True)
class Constraint(ABC):
    label: str

    @abstractmethod
    def to_cypher(self) -> str:
        pass


@dataclass(frozen=True, eq=True)
class MemgraphConstraintUnique(Constraint):
    property: Union[str, Tuple]

    def to_cypher(self) -> str:
        properties_str = ""
        if isinstance(self.property, (tuple, set, list)):
            properties_str = ", ".join([f"n.{prop}" for prop in self.property])
        else:
            properties_str = f"n.{self.property}"
        return f"(n:{self.label}) ASSERT {properties_str} IS UNIQUE"


@dataclass(frozen=True, eq=True)
class MemgraphConstraintExists(Constraint):
    property: str

    def to_cypher(self) -> str:
        return f"(n:{self.label}) ASSERT EXISTS (n.{self.property})"


@dataclass(frozen=True, eq=True)
class Neo4jConstraintUnique(Constraint):
    property: Union[str, Tuple]

    def to_cypher(self) -> str:
        properties_str = ""
        if isinstance(self.property, (tuple, set, list)):
            properties_str = ", ".join([f"n.{prop}" for prop in self.property])
        else:
            properties_str = f"n.{self.property}"
        return f"(n:{self.label}) ASSERT {properties_str} IS UNIQUE"


@dataclass(frozen=True, eq=True)
class Neo4jConstraintExists(Constraint):
    property: str

    def to_cypher(self) -> str:
        return f"(n:{self.label}) ASSERT EXISTS (n.{self.property})"
