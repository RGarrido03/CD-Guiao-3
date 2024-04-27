import json
from datetime import datetime
from socket import socket
import xml.etree.ElementTree as ET


class JSONUtils:
    @classmethod
    def encode(cls, message: dict) -> bytes:
        return json.dumps(message).encode("utf-8")

    @classmethod
    def decode(cls, message: bytes) -> dict:
        return json.loads(message.decode("utf-8"))


class Message:
    """Message Type."""

    def __init__(self, command):
        self.command = command


class Subscribe(Message):
    """Message to join a chat topic."""

    def __init__(self, command, type, topic):
        super().__init__(command)
        self.topic = topic
        self.type = type

    def dict(self):
        return {"command": self.command, "type": self.type.__str__(), "topic": self.topic}

    def __str__(self):
        return f'{{"command": "{self.command}", "type": "{self.type.__str__()}", "topic": "{self.topic}"}}'


class ListTopics(Message):
    def __init__(self, command, type):
        super().__init__(command)
        self.type = type

    def dict(self):
        return {"command": self.command, "type": self.type.__str__()}

    def __str__(self):
        return f'{{"command": "{self.command}", "type": "{self.type.__str__()}"}}'


class ListTopicsOK(Message):
    def __init__(self, command, topics):
        super().__init__(command)
        self.topics = topics

    def dict(self):
        return {"command": self.command, "type": self.type.__str__(), "topics": self.topics}

    def __str__(self):
        return f'{{"command": "{self.command}", "type": "{self.type.__str__()}", "topics": "{self.topics}"}}'


class Publish(Message):
    """Message to chat with other clients."""

    def __init__(self, command, type, topic, message):
        super().__init__(command)
        self.topic = topic
        self.message = message
        self.type = type

    def dict(self):
        return {"command": self.command, "type": self.type.__str__(), "topic": self.topic, "message": self.message}

    def __str__(self):
        return f'{{"command": "{self.command}", "type": "{self.type.__str__()}", "topic": "{self.topic}", "message": "{self.message}"}}'


class Unsubscribe(Message):
    def __init__(self, command, type, topic):
        super().__init__(command)
        self.topic = topic
        self.type = type

    def dict(self):
        return {"command": self.command, "type": self.type.__str__(), "topic": self.topic}

    def __str__(self):
        return f'{{"command": "{self.command}", "type": "{self.type.__str__()}", "topic": "{self.topic}"}}'


class JoinMessage(Message):
    """Message to join a chat channel."""

    # Class constructor initialization
    def __init__(self, command, channel):
        super().__init__(command)
        self.channel = channel

    # String representation of the object
    def __str__(self):
        return f'{{"command": "{self.command}", "channel": "{self.channel}"}}'


class RegisterMessage(Message):
    """Message to register username in the server."""

    # Class constructor initialization
    def __init__(self, command, username):
        super().__init__(command)
        self.username = username

    # String representation of the object
    def __str__(self):
        return f'{{"command": "{self.command}", "user": "{self.username}"}}'


class TextMessage(Message):
    """Message to chat with other clients."""

    # Class constructor initialization
    def __init__(self, command, message, channel=None, ts=None):
        super().__init__(command)
        self.message = message
        self.channel = channel
        self.ts = ts

    # String representation of the object
    def __str__(self):
        if self.channel:
            return f'{{"command": "{self.command}", "message": "{self.message}", "channel": "{self.channel}", "ts": {self.ts}}}'
        else:
            return f'{{"command": "{self.command}", "message": "{self.message}", "ts": {self.ts}}}'


class CDProto:
    """Computação Distribuida Protocol."""

    @classmethod
    def subscribe(self, type, topic) -> Subscribe:
        """Creates a SubscribeMessage object."""
        return Subscribe("subscribe", type, topic)

    @classmethod
    def publish(self, type, topic, message) -> Publish:
        """Creates a PublishMessage object."""
        return Publish("publish", type, topic, message)

    @classmethod
    def listTopics(self, type, list=None) -> ListTopics:
        """Creates a ListTopicsMessage object."""
        if list:
            return ListTopicsOK("listTopics", list)
        return ListTopics("listTopics", type)

    @classmethod
    def unsubscribe(self, type, topic) -> Unsubscribe:
        """Creates a UnsubscribeMessage object."""
        return Unsubscribe("unsubscribe", type, topic)

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
    def send_msg(self, connection: socket, command, _type="", topic="", message=None):
        print(command)
        if command == "subscribe":
            msg = self.subscribe(_type, topic)
        elif command == "publish":
            msg = self.publish(_type, topic, message)
        elif command == "listTopics":
            msg = self.listTopics(_type, message)
        elif command == "unsubscribe":
            msg = self.unsubscribe(_type, topic)

        if _type == "JSONQueue" or _type.__str__() == "Serializer.JSON":
            msg = json.dumps(msg.dict()).encode('utf-8')

        elif _type == "XMLQueue" or _type.__str__() == "Serializer.XML":
            msg = msg.dict()
            for key in msg:
                msg[key] = str(msg[key])
            msg = ET.tostring(ET.Element("message", msg))

        try:
            header = (len(msg)).to_bytes(2, byteorder="big")
            connection.send(header + msg)
        except:
            raise CDProtoBadFormat(msg)

    @classmethod
    def recv_msg(cls, connection: socket) -> Message:
        """Receives through a connection a Message object."""

        # Receive the message from the server and store it in a dictionary
        h = int.from_bytes(connection.recv(2), 'big')

        if h == 0:
            return None

        message = connection.recv(h).decode('utf-8')

        try:
            json_message = json.loads(message)

            if type(json_message) is not dict:  # Check if the json_message is dict type
                dictionary = json.loads(json_message)

            else:
                dictionary = json_message

        except json.JSONDecodeError:
            raise CDProtoBadFormat(message)

        if dictionary["command"] == "register":
            userName = dictionary["user"]
            return CDProto.register(userName)

        elif dictionary["command"] == "join":
            channel = dictionary["channel"]
            return CDProto.join(channel)

        elif dictionary["command"] == "message":
            msg = dictionary["message"]

            # Check if the channel atribute exists
            try:
                channel = dictionary["channel"]
            except KeyError:
                return CDProto.message(msg)

            return CDProto.message(msg, channel)


class CDProtoBadFormat(Exception):
    """Exception when source message is not CDProto."""

    def __init__(self, original_msg: bytes = None):
        """Store original message that triggered exception."""
        self._original = original_msg

    @property
    def original_msg(self) -> str:
        """Retrieve original message as a string."""
        return self._original.decode("utf-8")
