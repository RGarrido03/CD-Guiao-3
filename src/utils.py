import json
import pickle
import xml.etree.ElementTree as ET
from typing import Type, Union

from src.consts import Serializer


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


encoder_map: dict[Serializer, Type[Union[JsonUtils, XmlUtils, PickleUtils]]] = {
    Serializer.JSON: JsonUtils,
    Serializer.XML: XmlUtils,
    Serializer.PICKLE: PickleUtils,
}
