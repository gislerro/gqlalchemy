from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import List, Union


class TriggerEventType:
    """An enum representing types of trigger events."""

    CREATE = "CREATE"
    UPDATE = "UPDATE"
    DELETE = "DELETE"

    @classmethod
    def list(cls):
        return [cls.CREATE, cls.UPDATE, cls.DELETE]


class TriggerEventObject:
    """An enum representing types of trigger objects.

    NODE -> `()`
    RELATIONSHIP -> `-->`
    """

    NODE = "()"
    RELATIONSHIP = "-->"

    @classmethod
    def list(cls):
        return [cls.NODE, cls.RELATIONSHIP]


class TriggerExecutionPhase:
    """An enum representing types of trigger objects.

    Enum:
        BEFORE
        AFTER
    """

    BEFORE = "BEFORE"
    AFTER = "AFTER"


@dataclass(frozen=True, eq=True)
class MemgraphStream(ABC):
    name: str
    topics: List[str]
    transform: str

    @abstractmethod
    def to_cypher(self) -> str:
        pass


class MemgraphKafkaStream(MemgraphStream):
    """A class for creating and managing Kafka streams in Memgraph.

    Args:
        name: A string representing the stream name.
        topics: A list of strings representing the stream topics.
        transform: A string representing the name of the transformation procedure.
        consumer_group: A string representing the consumer group.
        name: A string representing the batch interval.
        name: A string representing the batch size.
        name: A string or list of strings representing bootstrap server addresses.
    """

    def __init__(
        self,
        name: str,
        topics: List[str],
        transform: str,
        consumer_group: str,
        batch_interval: str,
        batch_size: str,
        bootstrap_servers: Union[str, List[str]],
    ):
        super().__init__(name, topics, transform)
        self.consumer_group = consumer_group
        self.batch_interval = batch_interval
        self.batch_size = batch_size
        self.bootstrap_servers = bootstrap_servers

    def to_cypher(self) -> str:
        """Converts Kafka stream to a Cypher clause."""
        topics = ",".join(self.topics)
        query = f"CREATE KAFKA STREAM {self.name} TOPICS {topics} TRANSFORM {self.transform}"
        if self.consumer_group is not None:
            query += f" CONSUMER_GROUP {self.consumer_group}"
        if self.batch_interval is not None:
            query += f" BATCH_INTERVAL {self.batch_interval}"
        if self.batch_size is not None:
            query += f" BATCH_SIZE {self.batch_size}"
        if self.bootstrap_servers is not None:
            if isinstance(self.bootstrap_servers, str):
                servers_field = f"'{self.bootstrap_servers}'"
            else:
                servers_field = str(self.bootstrap_servers)[1:-1]
            query += f" BOOTSTRAP_SERVERS {servers_field}"
        query += ";"
        return query


class MemgraphPulsarStream(MemgraphStream):
    """A class for creating and managing Pulsar streams in Memgraph.

    Args:
        name: A string representing the stream name.
        topics: A list of strings representing the stream topics.
        transform: A string representing the name of the transformation procedure.
        consumer_group: A string representing the consumer group.
        name: A string representing the batch interval.
        name: A string representing the batch size.
        name: A string or list of strings representing bootstrap server addresses.
    """

    def __init__(
        self,
        name: str,
        topics: List[str],
        transform: str,
        batch_interval: str,
        batch_size: str,
        service_url: str,
    ):
        super().__init__(name, topics, transform)
        self.batch_interval = batch_interval
        self.batch_size = batch_size
        self.service_url = service_url

    def to_cypher(self) -> str:
        """Converts Pulsar stream to a Cypher clause."""
        topics = ",".join(self.topics)
        query = f"CREATE PULSAR STREAM {self.name} TOPICS {topics} TRANSFORM {self.transform}"
        if self.batch_interval is not None:
            query += f" BATCH_INTERVAL {self.batch_interval}"
        if self.batch_size is not None:
            query += f" BATCH_SIZE {self.batch_size}"
        if self.service_url is not None:
            query += f" SERVICE_URL {self.service_url}"
        query += ";"
        return query


@dataclass(frozen=True, eq=True)
class MemgraphTrigger:
    name: str
    execution_phase: TriggerExecutionPhase
    statement: str
    event_type: TriggerEventType | None = None
    event_object: TriggerEventObject | None = None

    def to_cypher(self) -> str:
        """Converts a Trigger to a cypher clause."""
        query = f"CREATE TRIGGER {self.name} "
        if self.event_type in TriggerEventType.list():
            query += "ON " + (
                f"{self.event_object} {self.event_type} "
                if self.event_object in TriggerEventObject.list()
                else f"{self.event_type} "
            )
        query += f"{self.execution_phase} COMMIT EXECUTE "
        query += f"{self.statement};"
        return query
