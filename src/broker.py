"""Message Broker"""

import selectors
import socket
from typing import List, Tuple, Union

from src.consts import Serializer, Command
from src.protocol import (
    CDProto,
    SubscribeTopic,
    PublishMessage,
    TopicList,
    UnsubscribeTopic,
)

subscriber_type = tuple[socket.socket, Serializer]
topic_type = tuple[list[subscriber_type], str]


class Broker:
    """Implementation of a PubSub Message Broker."""

    def __init__(self):
        """Initialize broker."""
        self.canceled = False
        self._host = "localhost"
        self._port = 5000

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind((self._host, self._port))
        self.socket.listen(100)

        self.sel = selectors.DefaultSelector()
        self.sel.register(self.socket, selectors.EVENT_READ, self.accept)

        """ publish com id = "root/node_id/leaf_id" 
        nome já diz o percurso
        ir nó a nó a ver quem está subscrito e enviar a mensagem
        podemos ter o parentId para fazer a search
        lista de subscribers (?)
        split last para ver o ultimo, ver quem esta subcrito e enviar, cortar, passar para o próximo, etc até ser vazio
        """

        """
        {
          "hello": (
            [
              (socket1, Serializer.JSON),
              (socket2, Serializer.XML),
              ...
            ],
            "abcd"
          )
          
        }
        
        JSON equivalent:
        {
          "hello": {
            "subscribers": [
              {
                "address": socket1,
                "serializer": Serializer.JSON
              },
              {
                "address": socket2,
                "serializer": Serializer.XML
              },
              ...
            ],
            "last_value": "abcd"
          }
        }
        """
        self.topics: dict[str, topic_type] = {}

    def accept(self, sock: socket.socket):
        conn, _ = sock.accept()
        conn.setblocking(False)
        self.sel.register(conn, selectors.EVENT_READ, self.read)

    def read(self, conn: socket.socket):
        msg = CDProto.recv_msg(conn)

        if msg is None:
            [self.unsubscribe(topic, conn) for topic in self.topics]
            self.sel.unregister(conn)
            conn.close()
            return

        msg, serializer = msg

        if isinstance(msg, SubscribeTopic):
            self.subscribe(msg.topic, conn, serializer)
            print("1:", msg.topic, serializer)
        elif isinstance(msg, PublishMessage):
            self.put_topic(msg.topic, msg.message)
            print("2:", msg.topic)
            topic = msg.topic.strip("/").split("/")
            while topic:
                msg.topic = "/".join(topic)
                for subscriber, _serializer in self.list_subscriptions(msg.topic):
                    CDProto.send_msg(
                        subscriber, Command.PUBLISH, _serializer, msg.topic, msg.message
                    )
                topic.pop()
        elif isinstance(msg, TopicList):
            CDProto.send_msg(
                conn, Command.TOPIC_LIST_SUCCESS, serializer, message=self.list_topics()
            )
        elif isinstance(msg, UnsubscribeTopic):
            self.unsubscribe(msg.topic, conn)

    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics containing values."""
        return list(self.topics.keys())

    def get_topic(self, topic) -> Union[str, None]:
        """Returns the currently stored value in topic."""
        if topic not in self.topics:
            return None
        return self.topics[topic][1]

    def put_topic(self, topic: str, value: str):
        """Store in topic the value."""
        if topic not in self.topics:
            self.topics[topic] = ([], value)
            return
        self.topics[topic] = self.topics[topic][0], value

    def list_subscriptions(self, topic: str) -> List[Tuple[socket.socket, Serializer]]:
        """Provide list of subscribers to a given topic."""
        if topic not in self.topics:
            return []
        return self.topics[topic][0]

    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        if topic not in self.topics:
            self.topics[topic] = ([], "")
        self.topics[topic][0].append((address, _format))
        if self.topics[topic][1] != "":
            CDProto.send_msg(
                address, Command.PUBLISH, _format, topic, self.topics[topic][1]
            )

    def unsubscribe(self, topic: str, address: socket.socket):
        """Unsubscribe to topic by client in address."""
        self.topics[topic] = (
            [client for client in self.topics[topic][0] if client[0] != address],
            self.topics[topic][1],
        )

    def run(self):
        """Run until canceled."""

        while not self.canceled:
            events = self.sel.select()
            for key, _ in events:
                callback = key.data
                callback(key.fileobj)
