from datetime import datetime
from enum import IntEnum
from socket import socket
from typing import Union

from src.consts import MiddlewareType
from src.utils import JsonUtils, XmlUtils, PickleUtils


class Message:
    """Message Type."""

    def __init__(self, command: str):
        self.command = command

    def to_dict(self) -> dict[str, str]:
        return {
            k: (v.value if isinstance(v, IntEnum) else v)
            for k, v in self.__dict__.items()
        }

    def __str__(self):
        return str(self.to_dict())


class JoinTopic(Message):
    """Message to join a chat topic."""

    def __init__(self, _type: MiddlewareType, topic: str):
        super().__init__("join_topic")
        self.type = _type
        self.topic = topic


class TopicList(Message):
    def __init__(self, _type: MiddlewareType):
        super().__init__("topic_list")
        self.type = _type


class TopicListSuccess(Message):
    def __init__(self, topics: list[str]):
        super().__init__("topic_list_success")
        self.topics = topics


class SendMessage(Message):
    """Message to chat with other clients."""

    def __init__(self, _type: MiddlewareType, topic: str, message: str):
        super().__init__("send_message")
        self.type = _type
        self.topic = topic
        self.message = message


class LeaveTopic(Message):
    def __init__(self, _type: MiddlewareType, topic: str):
        super().__init__("leave_topic")
        self.type = _type
        self.topic = topic


class JoinMessage(Message):
    """Message to join a chat channel."""

    def __init__(self, channel: str):
        super().__init__("join")
        self.channel = channel


class RegisterMessage(Message):
    """Message to register username in the server."""

    def __init__(self, username: str):
        super().__init__("register")
        self.username = username


class TextMessage(Message):
    """Message to chat with other clients."""

    def __init__(self, message: str, ts: int, channel: str = None):
        super().__init__("message")
        self.message = message
        self.channel = channel
        self.ts = ts

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if v is not None}


class CDProto:
    """Computação Distribuida Protocol."""

    @classmethod
    def join_topic(cls, _type: MiddlewareType, topic: str) -> JoinTopic:
        """Creates a JoinTopic object."""
        return JoinTopic(_type, topic)

    @classmethod
    def send_message(
        cls, _type: MiddlewareType, topic: str, message: str
    ) -> SendMessage:
        """Creates a SendMessage object."""
        return SendMessage(_type, topic, message)

    @classmethod
    def topic_list(
        cls, _type: MiddlewareType, _list: list[str] = None
    ) -> Union[TopicList, TopicListSuccess]:
        """Creates a TopicListMessage object."""
        if _list:
            return TopicListSuccess(_list)
        return TopicList(_type)

    @classmethod
    def leave_topic(cls, _type: MiddlewareType, topic: str) -> LeaveTopic:
        """Creates a LeaveTopic object."""
        return LeaveTopic(_type, topic)

    @classmethod
    def register(cls, username: str) -> RegisterMessage:
        """Creates a RegisterMessage object."""
        return RegisterMessage(username)

    @classmethod
    def join(cls, channel: str) -> JoinMessage:
        """Creates a JoinMessage object."""
        return JoinMessage(channel)

    @classmethod
    def message(cls, message: str, channel: str = None) -> TextMessage:
        """Creates a TextMessage object."""
        return TextMessage(message, int(datetime.now().timestamp()), channel)

    @classmethod
    def send_msg(
        cls,
        connection: socket,
        command: str,
        _type: MiddlewareType = None,
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
