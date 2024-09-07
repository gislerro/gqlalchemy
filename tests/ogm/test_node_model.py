import pytest

from pydantic import BaseModel, ConfigDict
from pydantic import Field as PydanticField

from gqlalchemy import Node as PythonNode, Field, GQLConfig, Memgraph


db = Memgraph()


# TODO: parametrize over multiple ConfigDicts
class DataModel(BaseModel):
    model_config = ConfigDict(allow_inf_nan=True)


# TODO: parametrize over class kwargs of Node
# TODO: parametrize over gql field metadata

# TODO: test labels for inherited pydantic models => new kwarg "inherit_label?"


### Start Pydantic Models ###
class Character(DataModel):
    name: str = PydanticField()
    description: str = PydanticField()
    foo: str = PydanticField(default="bar")


CHARACTER_KWARGS = {"name": "Alice", "description": "A character"}


class NestedCharacter(DataModel):
    character: Character = PydanticField(...)


NESTED_CHARACTER_KWARGS = {"character": Character(**CHARACTER_KWARGS)}


class Extension(DataModel):
    traits: str = PydanticField(...)


EXTENSION_KWARGS = {"traits": "A trait"}


class ExtendedCharacter(Character, Extension):
    pass


EXTENDEND_KWARGS = {**CHARACTER_KWARGS, **EXTENSION_KWARGS}


### End Pydantic Models ###


### Start Node Models ###
class Node(PythonNode, db=db, opaque=True):
    pass


class CharacterNode(Node, label="Foo", index=True, labels={"Bar", "Foobar"}):
    name: str = Field(GQLConfig(), default="Alice")
    description: str = Field(None, ...)


class NestedCharacterNode(Node, DataModel, db=db):
    character: CharacterNode = Field(GQLConfig(index=True), ...)


NESTED_CHARACTER_NODE_KWARGS = {"character": CharacterNode(**CHARACTER_KWARGS)}


class ExtendedCharacterNode(CharacterNode, Extension, db=db):
    pass


### End Node Models ###


### Start Inherited Node Models ###
class InheritedCharacterNode(Node, Character):
    pass


class InheritedNestedCharacterNode(Node, NestedCharacter):
    pass


foo = InheritedNestedCharacterNode(**NESTED_CHARACTER_KWARGS)


class InheritedExtendedCharacterNode(Node, ExtendedCharacter):
    pass


@pytest.mark.parametrize(
    ["model", "kwargs"],
    [
        (Character, CHARACTER_KWARGS),
        (NestedCharacter, NESTED_CHARACTER_KWARGS),
        (ExtendedCharacter, EXTENDEND_KWARGS),
        # nodes
        (CharacterNode, CHARACTER_KWARGS),
        (NestedCharacterNode, NESTED_CHARACTER_NODE_KWARGS),
        (ExtendedCharacterNode, EXTENDEND_KWARGS),
        # inherited nodes constructed from inherited pydantic models
        (InheritedCharacterNode, CHARACTER_KWARGS),
        (InheritedNestedCharacterNode, NESTED_CHARACTER_KWARGS),
        (InheritedExtendedCharacterNode, EXTENDEND_KWARGS),
    ],
)
def test_model_init(model, kwargs):
    assert model(**kwargs)


@pytest.mark.parametrize(
    "pydanticModel, gqlalchemyNode",
    [
        (Character(**CHARACTER_KWARGS), CharacterNode(**CHARACTER_KWARGS)),
        (NestedCharacter(**NESTED_CHARACTER_KWARGS), NestedCharacterNode(**NESTED_CHARACTER_NODE_KWARGS)),
        (ExtendedCharacter(**EXTENDEND_KWARGS), ExtendedCharacterNode(**EXTENDEND_KWARGS)),
        (Character(**CHARACTER_KWARGS), InheritedCharacterNode(**CHARACTER_KWARGS)),
        (
            NestedCharacter(**NESTED_CHARACTER_KWARGS),
            InheritedNestedCharacterNode(**NESTED_CHARACTER_KWARGS),
        ),
        (ExtendedCharacter(**EXTENDEND_KWARGS), InheritedExtendedCharacterNode(**EXTENDEND_KWARGS)),
    ],
)
class TestPydanticBehaviorPair:
    # https://docs.pydantic.dev/latest/concepts/models/#model-methods-and-properties

    def test_model_dump(self, pydanticModel: BaseModel, gqlalchemyNode: Node):
        assert pydanticModel.model_dump() == gqlalchemyNode.model_dump()

    def test_model_dump_json(self, pydanticModel: BaseModel, gqlalchemyNode: Node):
        assert pydanticModel.model_dump_json() == gqlalchemyNode.model_dump_json()

    def test_model_copy(self, pydanticModel: BaseModel, gqlalchemyNode: Node):
        assert pydanticModel.model_copy().model_dump() == gqlalchemyNode.model_copy().model_dump()

    def test_model_extra(self, pydanticModel: BaseModel, gqlalchemyNode: Node):
        assert pydanticModel.model_extra == gqlalchemyNode.model_extra

    def test_model_computed_fields(self, pydanticModel: BaseModel, gqlalchemyNode: Node):
        assert pydanticModel.model_computed_fields == gqlalchemyNode.model_computed_fields

    def test_model_fields_set(self, pydanticModel: BaseModel, gqlalchemyNode: Node):
        assert pydanticModel.model_fields_set == gqlalchemyNode.model_fields_set


@pytest.mark.parametrize(
    "gqlalchemyNode",
    [
        CharacterNode(**CHARACTER_KWARGS),
        NestedCharacterNode(**NESTED_CHARACTER_NODE_KWARGS),
        ExtendedCharacterNode(**EXTENDEND_KWARGS),
        InheritedCharacterNode(**CHARACTER_KWARGS),
        InheritedNestedCharacterNode(**NESTED_CHARACTER_KWARGS),
        InheritedExtendedCharacterNode(**EXTENDEND_KWARGS),
    ],
)
class TestPydanticBehavior:

    def test_model_validate(self, gqlalchemyNode: Node):
        dump = gqlalchemyNode.model_dump()
        gqlalchemyNode.model_validate(dump)

    def test_model_validate_json(self, gqlalchemyNode: Node):
        dump_json = gqlalchemyNode.model_dump_json()
        gqlalchemyNode.model_validate_json(dump_json)
