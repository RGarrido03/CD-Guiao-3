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

    SUBSCRIBE = "subscribe"
    PUBLISH = "publish"
    TOPIC_LIST = "topic_list"
    TOPIC_LIST_SUCCESS = "topic_list_success"
    UNSUBSCRIBE = "unsubscribe"
