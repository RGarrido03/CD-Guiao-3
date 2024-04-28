"""Middleware to communicate with PubSub Message Broker."""

import socket
from collections.abc import Callable
from typing import Any, Tuple

from src.consts import MiddlewareType, Serializer, Command
from src.protocol import CDProto


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, serializer: Serializer, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        self.topic = topic
        self.type = _type
        self.serializer = serializer

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(("localhost", 5000))

        if self.type == MiddlewareType.CONSUMER:
            CDProto.send_msg(self.sock, Command.SUBSCRIBE, self.serializer, self.topic)

    def push(self, value):
        """Sends data to broker."""
        CDProto.send_msg(self.sock, Command.PUBLISH, self.serializer, self.topic, value)

    def pull(self) -> Tuple[str, Any]:
        """Receives (topic, data) from broker. Should BLOCK the consumer!"""
        msg = CDProto.recv_msg(self.sock)
        if msg.command == "publish":
            return msg
        elif msg.command == "listTopics":
            # invocar callback
            pass

    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        CDProto.send_msg(self.sock, Command.TOPIC_LIST, self.serializer, self.topic)
        callback()

    def cancel(self):
        """Cancel subscription."""
        CDProto.send_msg(self.sock, Command.UNSUBSCRIBE, self.serializer, self.topic)


class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, Serializer.JSON, _type)


class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, Serializer.XML, _type)


class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, Serializer.PICKLE, _type)
