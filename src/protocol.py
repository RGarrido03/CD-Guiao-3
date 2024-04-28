from datetime import datetime
from enum import Enum
from socket import socket
from typing import Union

from src.consts import Serializer, Command
from src.utils import encoder_map


class Message:
    """Message Type."""

    def __init__(self, command: Command):
        self.command = command

    def to_dict(self) -> dict[str, str]:
        return {
            k: (v.value if isinstance(v, Enum) else v) for k, v in self.__dict__.items()
        }

    def __str__(self):
        return str(self.to_dict())


class JoinTopic(Message):
    """Message to join a chat topic."""

    def __init__(self, topic: str):
        super().__init__(Command.JOIN_TOPIC)
        self.topic = topic


class TopicList(Message):
    def __init__(self):
        super().__init__(Command.TOPIC_LIST)


class TopicListSuccess(Message):
    def __init__(self, topics: list[str]):
        super().__init__(Command.TOPIC_LIST_SUCCESS)
        self.topics = topics


class SendMessage(Message):
    """Message to chat with other clients."""

    def __init__(self, topic: str, message: str):
        super().__init__(Command.SEND_MESSAGE)
        self.topic = topic
        self.message = message


class LeaveTopic(Message):
    def __init__(self, topic: str):
        super().__init__(Command.LEAVE_TOPIC)
        self.topic = topic


class JoinMessage(Message):
    """Message to join a chat channel."""

    def __init__(self, channel: str):
        super().__init__(Command.JOIN)
        self.channel = channel


class RegisterMessage(Message):
    """Message to register username in the server."""

    def __init__(self, username: str):
        super().__init__(Command.REGISTER)
        self.username = username


class TextMessage(Message):
    """Message to chat with other clients."""

    def __init__(self, message: str, ts: int, channel: str = None):
        super().__init__(Command.MESSAGE)
        self.message = message
        self.channel = channel
        self.ts = ts

    def to_dict(self):
        return {k: v for k, v in self.__dict__.items() if v is not None}


class CDProto:
    """Computação Distribuida Protocol."""

    @classmethod
    def join_topic(cls, topic: str) -> JoinTopic:
        """Creates a JoinTopic object."""
        return JoinTopic(topic)

    @classmethod
    def send_message(cls, topic: str, message: str) -> SendMessage:
        """Creates a SendMessage object."""
        return SendMessage(topic, message)

    @classmethod
    def topic_list(cls, _list: list[str] = None) -> Union[TopicList, TopicListSuccess]:
        """Creates a TopicListMessage object."""
        if _list:
            return TopicListSuccess(_list)
        return TopicList()

    @classmethod
    def leave_topic(cls, topic: str) -> LeaveTopic:
        """Creates a LeaveTopic object."""
        return LeaveTopic(topic)

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
        command: Command,
        _type: Serializer = None,
        topic: str = "",
        message: str = None,
    ) -> None:
        """Sends a message to the broker based on the command type."""
        try:
            if command == Command.JOIN_TOPIC:
                msg = cls.join_topic(topic)
            elif command == Command.PUBLISH:
                msg = cls.send_message(topic, message)
            elif command == Command.LIST_TOPICS:
                msg = cls.topic_list(message)
            elif command == Command.UNSUBSCRIBE:
                msg = cls.leave_topic(topic)
            else:
                raise ValueError(f"Unsupported command: {command}")

            msg = encoder_map[_type].encode(msg.to_dict())

            header = (len(msg) + 2).to_bytes(2, byteorder="big")
            serializer = _type.value.to_bytes(2, byteorder="big")
            connection.send(header + serializer + msg)
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

            serializer = Serializer(int.from_bytes(connection.recv(2), "big"))

            message = connection.recv(h - 2)
            dictionary = encoder_map[serializer].decode(message)
            command = Command(dictionary["command"])
            print("command", command.name)

            if command == Command.REGISTER:
                user_name = dictionary["user"]
                return CDProto.register(user_name)
            elif command == Command.JOIN:
                channel = dictionary["channel"]
                return CDProto.join(channel)
            elif command == Command.MESSAGE:
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
