import json
import pickle
import xml.etree.ElementTree as ET
from datetime import datetime
from socket import socket
from typing import Union


class JsonUtils:
    @classmethod
    def encode(cls, message: dict) -> bytes:
        return json.dumps(message).encode("utf-8")

    @classmethod
    def decode(cls, message: bytes) -> dict:
        return json.loads(message.decode("utf-8"))


class XmlUtils:
    @classmethod
    def encode(cls, message: dict) -> bytes:
        for key in message:
            message[key] = str(message[key])

        return ET.tostring(ET.Element("message", message))

    @classmethod
    def decode(cls, message: bytes) -> dict:
        return ET.XML(message.decode("utf-8")).attrib


class PickleUtils:
    @classmethod
    def encode(cls, message: dict) -> bytes:
        return pickle.dumps(message)

    @classmethod
    def decode(cls, message: bytes) -> dict:
        return pickle.loads(message)


class Message:
    """Message Type."""

    def __init__(self, command: str):
        self.command = command

    def to_dict(self) -> dict[str, str]:
        return self.__dict__

    def __str__(self):
        return str(self.to_dict())


class JoinTopic(Message):
    """Message to join a chat topic."""

    def __init__(self, command: str, _type: str, topic: str):
        super().__init__(command)
        self.type = _type
        self.topic = topic


class TopicList(Message):
    def __init__(self, command: str, _type: str):
        super().__init__(command)
        self.type = _type


class TopicListSuccess(Message):
    def __init__(self, command: str, topics: list[str]):
        super().__init__(command)
        self.topics = topics


class SendMessage(Message):
    """Message to chat with other clients."""

    def __init__(self, command: str, _type: str, topic: str, message: str):
        super().__init__(command)
        self.type = _type
        self.topic = topic
        self.message = message


class LeaveTopic(Message):
    def __init__(self, command: str, _type: str, topic: str):
        super().__init__(command)
        self.type = _type
        self.topic = topic


class JoinMessage(Message):
    """Message to join a chat channel."""

    def __init__(self, command: str, channel: str):
        super().__init__(command)
        self.channel = channel


class RegisterMessage(Message):
    """Message to register username in the server."""

    def __init__(self, command: str, username: str):
        super().__init__(command)
        self.username = username


class TextMessage(Message):
    """Message to chat with other clients."""

    def __init__(self, command: str, message: str, channel: str = None, ts: int = None):
        super().__init__(command)
        self.message = message
        self.channel = channel
        self.ts = ts

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if v is not None}


class CDProto:
    """Computação Distribuida Protocol."""

    @classmethod
    def join_topic(cls, _type: str, topic: str) -> JoinTopic:
        """Creates a JoinTopic object."""
        return JoinTopic("join_topic", _type, topic)

    @classmethod
    def send_message(cls, _type: str, topic: str, message: str) -> SendMessage:
        """Creates a SendMessage object."""
        return SendMessage("send_message", _type, topic, message)

    @classmethod
    def topic_list(
        cls, _type: str, _list: list[str] = None
    ) -> Union[TopicList, TopicListSuccess]:
        """Creates a TopicListMessage object."""
        if _list:
            return TopicListSuccess("topic_list_success", _list)
        return TopicList("topic_list", _type)

    @classmethod
    def leave_topic(cls, _type: str, topic: str) -> LeaveTopic:
        """Creates a LeaveTopic object."""
        return LeaveTopic("leave_topic", _type, topic)

    @classmethod
    def register(cls, username: str) -> RegisterMessage:
        """Creates a RegisterMessage object."""
        return RegisterMessage("register", username)

    @classmethod
    def join(cls, channel: str) -> JoinMessage:
        """Creates a JoinMessage object."""
        return JoinMessage("join", channel)

    @classmethod
    def message(cls, message: str, channel: str = None) -> TextMessage:
        """Creates a TextMessage object."""
        return TextMessage("message", message, channel, int(datetime.now().timestamp()))

    @classmethod
    def send_msg(
        cls,
        connection: socket,
        command: str,
        _type: str = "",
        topic: str = "",
        message: str = None,
    ) -> None:
        """Sends a message to the broker based on the command type."""
        try:
            if command == "subscribe":
                msg = cls.join_topic(_type, topic)
            elif command == "publish":
                msg = cls.send_message(_type, topic, message)
            elif command == "listTopics":
                msg = cls.topic_list(_type, message)
            elif command == "unsubscribe":
                msg = cls.leave_topic(_type, topic)
            else:
                raise ValueError(f"Unsupported command: {command}")

            if _type in ["JSONQueue", "Serializer.JSON"]:
                msg = JsonUtils.encode(msg.to_dict())
            elif _type in ["XMLQueue", "Serializer.XML"]:
                msg = XmlUtils.encode(msg.to_dict())
            elif _type in ["PickleQueue", "Serializer.PICKLE"]:
                msg = PickleUtils.encode(msg.to_dict())
            else:
                raise ValueError(f"Unsupported serialization type: {_type}")

            header = (len(msg)).to_bytes(2, byteorder="big")
            connection.send(header + msg)
        except Exception as e:
            raise CDProtoBadFormat(f"Error sending message: {e}")

    @classmethod
    def recv_msg(cls, connection: socket) -> Union[Message, None]:
        """Receives through a connection a Message object."""
        try:
            # Receive the message length header
            h = int.from_bytes(connection.recv(2), "big")
            if h == 0:
                return None

            message = connection.recv(h)
            dictionary = JsonUtils.decode(message)

            if dictionary["command"] == "register":
                user_name = dictionary["user"]
                return CDProto.register(user_name)
            elif dictionary["command"] == "join":
                channel = dictionary["channel"]
                return CDProto.join(channel)
            elif dictionary["command"] == "message":
                msg = dictionary["message"]
                channel = dictionary.get("channel")
                if channel is None:
                    return CDProto.message(msg)
                return CDProto.message(msg, channel)
            else:
                raise ValueError(f"Unsupported command: {dictionary['command']}")
        except Exception as e:
            raise CDProtoBadFormat(f"Error receiving message: {e}")


class CDProtoBadFormat(Exception):
    """Exception when the source message is not CDProto."""

    def __init__(self, original_msg: str = None):
        """Store the original message that triggered exception."""
        self._original = original_msg

    @property
    def original_msg(self) -> str:
        """Retrieve the original message as a string."""
        return self._original
