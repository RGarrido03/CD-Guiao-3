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


class SubscribeTopic(Message):
    def __init__(self, topic: str):
        super().__init__(Command.SUBSCRIBE)
        self.topic = topic


class PublishMessage(Message):
    def __init__(self, topic: str, message: str):
        super().__init__(Command.PUBLISH)
        self.topic = topic
        self.message = message


class TopicList(Message):
    def __init__(self):
        super().__init__(Command.TOPIC_LIST)


class TopicListSuccess(Message):
    def __init__(self, topics: list[str]):
        super().__init__(Command.TOPIC_LIST_SUCCESS)
        self.topics = topics


class UnsubscribeTopic(Message):
    def __init__(self, topic: str):
        super().__init__(Command.UNSUBSCRIBE)
        self.topic = topic


class CDProto:
    """Computação Distribuida Protocol."""

    @classmethod
    def subscribe_topic(cls, topic: str) -> SubscribeTopic:
        """Creates a SubscribeTopic object."""
        return SubscribeTopic(topic)

    @classmethod
    def publish_message(cls, topic: str, message: str) -> PublishMessage:
        """Creates a PublishMessage object."""
        return PublishMessage(topic, message)

    @classmethod
    def topic_list(cls, _list: list[str] = None) -> Union[TopicList, TopicListSuccess]:
        """Creates a TopicListMessage object."""
        if _list:
            return TopicListSuccess(_list)
        return TopicList()

    @classmethod
    def unsubscribe_topic(cls, topic: str) -> UnsubscribeTopic:
        """Creates a UnsubscribeTopic object."""
        return UnsubscribeTopic(topic)

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
            if command == Command.SUBSCRIBE:
                msg = cls.subscribe_topic(topic)
            elif command == Command.PUBLISH:
                msg = cls.publish_message(topic, message)
            elif command == Command.TOPIC_LIST:
                msg = cls.topic_list(message)
            elif command == Command.UNSUBSCRIBE:
                msg = cls.unsubscribe_topic(topic)
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

            if command == Command.SUBSCRIBE:
                return CDProto.subscribe_topic(dictionary["topic"])
            elif command == Command.PUBLISH:
                return CDProto.publish_message(
                    dictionary["topic"], dictionary["message"]
                )
            elif command == Command.TOPIC_LIST:
                return CDProto.topic_list()
            elif command == Command.UNSUBSCRIBE:
                return CDProto.unsubscribe_topic(dictionary["topic"])
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
