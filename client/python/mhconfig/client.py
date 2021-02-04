import time
import traceback
from collections import namedtuple
from dataclasses import dataclass
from enum import Enum, IntEnum
from queue import Queue
from threading import Event, Lock, Thread
from typing import Any, Dict, List, Optional, Tuple

import grpc

from mhconfig.proto import mhconfig_pb2
from mhconfig.proto.mhconfig_pb2 import Element as ProtoElement


class UndefinedElement(object):
    def __eq__(self, other):
        return isinstance(other, UndefinedElement)

    def __ne__(self, other):
        return not isinstance(other, UndefinedElement)


class ServerChangedException(Exception):
    pass


@dataclass
class Position:
    source_id: int
    line: int
    col: int


@dataclass
class Element:
    val: Any
    pos: Optional[Position] = None


def decode_element(
    elements: List[ProtoElement],
    with_position: bool = False,
) -> Any:
    if not elements:
        return UndefinedElement()
    builder = _decode_element_with_position if with_position else _decode_element
    return builder(builder, elements, 0)


def _decode_element(
    builder,
    elements: List[ProtoElement],
    idx: int
) -> Any:
    element = elements[idx]
    if element.value_type == ProtoElement.ValueType.STR:
        return element.value_str
    elif element.value_type == ProtoElement.ValueType.BIN:
        return element.value_bin
    elif element.value_type == ProtoElement.ValueType.INT64:
        return element.value_int
    elif element.value_type == ProtoElement.ValueType.DOUBLE:
        return element.value_double
    elif element.value_type == ProtoElement.ValueType.BOOL:
        return element.value_bool
    elif element.value_type == ProtoElement.ValueType.MAP:
        result = {}
        for _ in range(element.size):
            idx += 1
            if elements[idx].key_type == ProtoElement.KeyType.KSTR:
                result[elements[idx].key_str] = builder(builder, elements, idx)
            else:
                raise NotImplementedError
            idx += elements[idx].sibling_offset
        return result
    elif element.value_type == ProtoElement.ValueType.SEQUENCE:
        result = []
        for _ in range(element.size):
            idx += 1
            result.append(builder(builder, elements, idx))
            idx += elements[idx].sibling_offset
        return result
    elif element.value_type == ProtoElement.ValueType.NONE:
        return None
    elif element.value_type == ProtoElement.ValueType.UNDEFINED:
        return UndefinedElement()

    raise NotImplementedError


def _decode_element_with_position(
    builder,
    elements: List[ProtoElement],
    idx: int,
) -> Any:
    value = _decode_element(builder, elements, idx)
    pos_proto = elements[idx].position
    position = decode_position(pos_proto)
    return Element(val=value, pos=position)


def decode_position(position):
    return Position(
        source_id=position.source_id,
        line=position.line,
        col=position.col,
    ) if position.present else None


class LogLevel(Enum):
    ERROR = 0
    WARN = 1
    INFO = 2
    DEBUG = 3
    TRACE = 4

    def to_proto(self):
        return mhconfig_pb2.LogLevel.Value(self.name)


@dataclass
class Log:
    level: LogLevel
    message: str
    position: Optional[Position] = None
    origin: Optional[Position] = None


def decode_log(log):
    return Log(
        level=decode_log_level(log.level),
        message=log.message,
        position=decode_position(log.position),
        origin=decode_position(log.origin)
    )


def decode_log_level(log_level):
    return LogLevel(log_level)


@dataclass
class Source:
    id: int
    checksum: int
    path: str


def decode_source(source):
    return Source(
        id=source.id,
        checksum=source.checksum,
        path=source.path,
    )


class Client:
    def __init__(
            self,
            stub,
            metadata: Tuple[Tuple[str, str]] = None,
    ):
        super().__init__()
        self._stub = stub
        self._metadata = metadata

    def get(
            self,
            root_path: str,
            labels: Dict[str, str],
            document: str,
            version=None,
            log_level=LogLevel.ERROR,
            with_position=False,
    ):
        proto_labels = [
            mhconfig_pb2.Label(key=k, value=v)
            for k, v in labels.items()
        ]
        request = mhconfig_pb2.GetRequest(
            root_path=root_path,
            labels=proto_labels,
            document=document,
            version=0 if version is None else version,
            log_level=log_level.to_proto()
        )
        r = self._stub.Get(request, metadata=self._metadata)

        value = decode_element(
            r.elements,
            with_position=with_position,
        )
        logs = [decode_log(x) for x in r.logs]
        sources = {x.id: decode_source(x) for x in r.sources}

        return (value, logs, sources)



