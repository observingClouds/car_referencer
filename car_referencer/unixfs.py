"""Minimal decoder for dag-pb nodes with UnixFS v1 data.

Only decoding is implemented, and only the fields needed to walk a UnixFS
DAG. See https://ipld.io/specs/codecs/dag-pb/spec/ and
https://github.com/ipfs/specs/blob/main/UNIXFS.md
"""

from dataclasses import dataclass, field
from enum import IntEnum
from typing import List, Optional

# protobuf wire types
VARINT = 0
I64 = 1
LEN = 2
I32 = 5


def _uvarint(buf, pos):
    value = shift = 0
    while True:
        if pos >= len(buf):
            raise ValueError("truncated varint")
        byte = buf[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, pos
        shift += 7
        if shift >= 64:
            raise ValueError("varint too long")


def _fields(buf):
    """Yield ``(field_number, wire_type, value)`` of a protobuf message.

    ``value`` is an int for varints and fixed-size fields, and bytes for
    length-delimited fields.
    """
    buf = memoryview(buf)
    pos = 0
    while pos < len(buf):
        key, pos = _uvarint(buf, pos)
        number, wire_type = key >> 3, key & 7
        if wire_type == VARINT:
            value, pos = _uvarint(buf, pos)
        elif wire_type == LEN:
            length, pos = _uvarint(buf, pos)
            if pos + length > len(buf):
                raise ValueError("truncated length-delimited field")
            value = bytes(buf[pos : pos + length])
            pos += length
        elif wire_type == I64:
            value = int.from_bytes(buf[pos : pos + 8], "little")
            pos += 8
        elif wire_type == I32:
            value = int.from_bytes(buf[pos : pos + 4], "little")
            pos += 4
        else:
            raise ValueError(f"unsupported protobuf wire type {wire_type}")
        yield number, wire_type, value


class DataType(IntEnum):
    Raw = 0
    Directory = 1
    File = 2
    Metadata = 3
    Symlink = 4
    HAMTShard = 5


@dataclass
class PBLink:
    Hash: Optional[bytes] = None
    Name: Optional[str] = None
    Tsize: Optional[int] = None

    @classmethod
    def loads(cls, buf):
        link = cls()
        for number, wire_type, value in _fields(buf):
            if number == 1 and wire_type == LEN:
                link.Hash = value
            elif number == 2 and wire_type == LEN:
                link.Name = value.decode("utf-8")
            elif number == 3 and wire_type == VARINT:
                link.Tsize = value
        return link


@dataclass
class PBNode:
    Links: List[PBLink] = field(default_factory=list)
    Data: Optional[bytes] = None

    @classmethod
    def loads(cls, buf):
        node = cls()
        for number, wire_type, value in _fields(buf):
            if number == 2 and wire_type == LEN:
                node.Links.append(PBLink.loads(value))
            elif number == 1 and wire_type == LEN:
                node.Data = value
        return node


@dataclass
class Data:
    """UnixFS v1 ``Data`` message (without ``mtime``)."""

    Type: DataType = DataType.Raw
    Data: Optional[bytes] = None
    filesize: Optional[int] = None
    blocksizes: List[int] = field(default_factory=list)
    hashType: Optional[int] = None
    fanout: Optional[int] = None
    mode: Optional[int] = None

    @classmethod
    def loads(cls, buf):
        data = cls()
        for number, wire_type, value in _fields(buf or b""):
            if number == 1 and wire_type == VARINT:
                data.Type = DataType(value)
            elif number == 2 and wire_type == LEN:
                data.Data = value
            elif number == 3 and wire_type == VARINT:
                data.filesize = value
            elif number == 4 and wire_type == VARINT:
                data.blocksizes.append(value)
            elif number == 4 and wire_type == LEN:  # packed encoding
                pos = 0
                while pos < len(value):
                    size, pos = _uvarint(value, pos)
                    data.blocksizes.append(size)
            elif number == 5 and wire_type == VARINT:
                data.hashType = value
            elif number == 6 and wire_type == VARINT:
                data.fanout = value
            elif number == 7 and wire_type == VARINT:
                data.mode = value
        return data
