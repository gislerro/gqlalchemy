from dataclasses import dataclass
from typing import Iterable

from gqlalchemy.exceptions import GQLAlchemyError
from gqlalchemy.models.node import Node
from gqlalchemy.models.relationship import Relationship


# TODO?: this doesn't have to be a GraphObject - can just be a plain dataclass
@dataclass
class Path:
    nodes: Iterable[Node]
    relationships: Iterable[Relationship]

    def __post_init__(self):
        if len(self.nodes) != len(self.relationships) + 1:
            raise GQLAlchemyError({"message": "Invalid path"})

    def traverse(self):
        yield self.nodes[0]
        for rel, node in zip(self.relationships, self.nodes):
            yield rel
            yield node

    def __str__(self) -> str:
        return "".join(
            (
                f"<{type(self).__name__}",
                f" nodes={self.nodes}",
                f" relationships={self.relationships}" ">",
            )
        )
