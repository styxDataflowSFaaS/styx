from enum import Enum, auto

import msgpack
import cloudpickle
import gzip


class Serializer(Enum):
    CLOUDPICKLE = auto()
    MSGPACK = auto()


def msgpack_serialization(serializable_object: object) -> bytes:
    return msgpack.packb(serializable_object)


def msgpack_deserialization(serialized_object: bytes) -> dict:
    return msgpack.unpackb(serialized_object)


def compressed_msgpack_serialization(serializable_object: object) -> bytes:
    return gzip.compress(msgpack.packb(serializable_object))


def compressed_msgpack_deserialization(serialized_object: bytes) -> dict:
    return msgpack.unpackb(gzip.decompress(serialized_object))


def cloudpickle_serialization(serializable_object: object) -> bytes:
    return cloudpickle.dumps(serializable_object)


def cloudpickle_deserialization(serialized_object: bytes) -> dict:
    return cloudpickle.loads(serialized_object)


def compressed_cloudpickle_serialization(serializable_object: object) -> bytes:
    return gzip.compress(cloudpickle.dumps(serializable_object))


def compressed_cloudpickle_deserialization(serialized_object: bytes) -> dict:
    return cloudpickle.loads(gzip.decompress(serialized_object))
