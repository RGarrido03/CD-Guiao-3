"""Middleware to communicate with PubSub Message Broker."""

import socket
from collections.abc import Callable
from typing import Any, Tuple

from src.consts import MiddlewareType
from src.utils import CDProto


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        self.topic = topic
        self.type = _type
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(("localhost", 5000))

        if self.type == MiddlewareType.CONSUMER:
            submsg = CDProto.join_topic(self.type, self.topic)
            CDProto.send_msg(self.sock, submsg)

    def push(self, value):
        """Sends data to broker."""

    def pull(self) -> Tuple[str, Any]:
        """Receives (topic, data) from broker. Should BLOCK the consumer!"""
        msg = CDProto.recv_msg(self.sock)
        if msg:  # pub
            return msg
        # elif: # list topcs
        #   pass
        # invocar callback
        # else:

    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        CDProto.send_msg(self.sock, CDProto.list_topics())  # ainda falta implementar
        callback

    def cancel(self):
        """Cancel subscription."""
        CDProto.send_msg(
            self.sock, CDProto.cancel(self.topic)
        )  # também falta implementar


class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.ser_type = "JSONQueue"

    def push(self, value):
        """Sends data to broker."""
        CDProto.send_msg(self.sock, "publish", self.ser_type, self.topic, value)

    def pull(self) -> Tuple[str, Any]:
        """Receives (topic, data) from broker. Should BLOCK the consumer!"""
        msg = CDProto.recv_msg(self.sock)
        if msg.command == "publish":
            return msg
        elif msg.command == "listTopics":
            # invocar callback
            pass
        # else:

    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        CDProto.send_msg(self.sock, "listTopics", self.ser_type, self.topic)
        callback

    def cancel(self):
        """Cancel subscription."""
        CDProto.send_msg(self.sock, "unsubscribe", self.ser_type, self.topic)


class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.ser_type = "XMLQueue"

    def push(self, value):
        """Sends data to broker."""
        CDProto.send_msg(self.sock, "publish", self.ser_type, self.topic, value)

    def pull(self) -> Tuple[str, Any]:
        """Receives (topic, data) from broker. Should BLOCK the consumer!"""
        msg = CDProto.recv_msg(self.sock)
        if msg.command == "publish":
            return msg
        elif msg.command == "listTopics":
            # invocar callback
            pass
        # else:

    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        CDProto.send_msg(self.sock, "listTopics", self.ser_type, self.topic)
        callback

    def cancel(self):
        """Cancel subscription."""
        CDProto.send_msg(self.sock, "unsubscribe", self.ser_type, self.topic)


class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        super().__init__(topic, _type)
        self.ser_type = "PickleQueue"

    def push(self, value):
        """Sends data to broker."""
        CDProto.send_msg(self.sock, "publish", self.ser_type, self.topic, value)

    def pull(self) -> Tuple[str, Any]:
        """Receives (topic, data) from broker. Should BLOCK the consumer!"""
        msg = CDProto.recv_msg(self.sock)
        if msg.command == "publish":
            return msg
        elif msg.command == "listTopics":
            # invocar callback
            pass
        # else:

    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        CDProto.send_msg(self.sock, "listTopics", self.ser_type, self.topic)
        callback

    def cancel(self):
        """Cancel subscription."""
        CDProto.send_msg(self.sock, "unsubscribe", self.ser_type, self.topic)
