from enum import IntEnum, Enum


class MiddlewareType(IntEnum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Serializer(IntEnum):
    """Possible message serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2


class Command(Enum):
    """Possible commands."""

    JOIN_TOPIC = "join_topic"
    TOPIC_LIST = "topic_list"
    TOPIC_LIST_SUCCESS = "topic_list_success"
    SEND_MESSAGE = "send_message"
    LEAVE_TOPIC = "leave_topic"
    JOIN = "join"
    REGISTER = "register"
    MESSAGE = "message"
    PUBLISH = "publish"
    LIST_TOPICS = "listTopics"
    SUBSCRIBE = "subscribe"
    UNSUBSCRIBE = "unsubscribe"
    ERROR = "error"
