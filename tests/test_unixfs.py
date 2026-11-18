"""Tests for the CARv1 reader and the UnixFS decoder."""

import pytest
from multiformats import varint

import car_referencer.index as idx
from car_referencer import unixfs

from .car_utils import (
    assert_matches_ipfsspec,
    carv1_bytes,
    directory_node,
    file_node,
    raw_block,
    write_carv1,
)


def test_test_car_matches_ipfsspec():
    assert assert_matches_ipfsspec("tests/test.car") > 0


def test_synthetic_car_matches_ipfsspec(tmp_path):
    a = raw_block(b"first chunk|")
    b = raw_block(b"second chunk|")
    chunked = file_node([a, b, a])
    root = directory_node({"chunked": chunked, "ünïcödé name": file_node([b])})
    car = tmp_path / "test.car"
    write_carv1(car, [root[0]], [root, chunked, a, b])
    assert assert_matches_ipfsspec(car) == 2


def test_packed_blocksizes():
    # Data with Type=File and blocksizes [3, 300] in packed encoding
    sizes = varint.encode(3) + varint.encode(300)
    buf = b"\x08\x02" + b"\x22" + varint.encode(len(sizes)) + sizes
    data = unixfs.Data.loads(buf)
    assert data.Type == unixfs.DataType.File
    assert data.blocksizes == [3, 300]


def test_unknown_fields_are_skipped():
    # fields 15 (varint), 16 (64 bit), 17 (32 bit) and 18 (bytes) followed
    # by Type=Directory
    buf = (
        b"\x78\x01"
        + b"\x81\x01"
        + bytes(8)
        + b"\x8d\x01"
        + bytes(4)
        + b"\x92\x01\x02ab"
        + b"\x08\x01"
    )
    assert unixfs.Data.loads(buf).Type == unixfs.DataType.Directory


def test_empty_data():
    data = unixfs.Data.loads(None)
    assert data.Type == unixfs.DataType.Raw
    assert unixfs.PBNode.loads(b"") == unixfs.PBNode()


@pytest.mark.parametrize("buf", [b"\x0a\x05ab", b"\x80", b"\x0b"])
def test_invalid_protobuf(buf):
    with pytest.raises(ValueError):
        unixfs.PBNode.loads(buf)


def test_verify_detects_corrupted_block(tmp_path):
    block = raw_block(b"some data")
    content = bytearray(carv1_bytes([block[0]], [block]))
    content[-1] ^= 0xFF  # corrupt the last byte of the block
    car = tmp_path / "corrupted.car"
    car.write_bytes(bytes(content))

    assert len(list(idx.read_car(str(car)))) == 1
    with pytest.raises(ValueError, match="could not be verified"):
        list(idx.read_car(str(car), verify=True))
    with pytest.raises(ValueError, match="could not be verified"):
        idx.generate_index([str(car)], verify=True)


def test_zero_length_section_ends_car(tmp_path):
    block = raw_block(b"some data")
    car = tmp_path / "padded.car"
    car.write_bytes(carv1_bytes([block[0]], [block]) + bytes(16))
    assert len(list(idx.read_car(str(car), verify=True))) == 1


def test_invalid_header(tmp_path):
    car = tmp_path / "invalid.car"
    car.write_bytes(b"\x01\xa0")  # header {} without version
    with pytest.raises(ValueError, match="valid CARv1 header"):
        list(idx.read_car(str(car)))
