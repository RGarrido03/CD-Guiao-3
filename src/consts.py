from enum import IntEnum


class MiddlewareType(IntEnum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Serializer(IntEnum):
    """Possible message serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2
