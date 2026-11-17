"""Tests for CARv2 support."""

import struct

import fsspec
import pytest
from multiformats import varint

import car_referencer.index as idx
import car_referencer.reference as ref

from .car_utils import carv1_bytes, directory_node, file_node, raw_block, write_carv1


def _multiwidth_index(entries):
    """Serialize ``(key, offset)`` entries as go-car multi-width index."""
    buckets = {}
    for key, offset in entries:
        buckets.setdefault(len(key) + 8, []).append((key, offset))
    out = struct.pack("<i", len(buckets))
    for width, bucket in sorted(buckets.items()):
        body = b"".join(key + struct.pack("<Q", off) for key, off in sorted(bucket))
        out += struct.pack("<Iq", width, len(body)) + body
    return out


def carv2_bytes(roots, blocks, index="multihash", padding=0):
    """Serialize ``blocks`` as CARv2.

    index : "multihash" (MultihashIndexSorted), "sorted" (IndexSorted) or None
    padding : number of zero bytes between header and data and between data
      and index
    """
    data = carv1_bytes(roots, blocks)
    # section offsets relative to the start of the data payload
    header_size, _, _ = varint.decode_raw(data)
    pos = len(varint.encode(header_size)) + header_size
    entries = []
    for cid, block in blocks:
        entries.append((cid, pos))
        section_size = len(bytes(cid)) + len(block)
        pos += len(varint.encode(section_size)) + section_size

    data_offset = len(idx.CARV2_PRAGMA) + idx.CARV2_HEADER_SIZE + padding
    if index is None:
        index_bytes = b""
        index_offset = 0
    else:
        index_offset = data_offset + len(data) + padding
        if index == "sorted":
            index_bytes = varint.encode(idx.INDEX_SORTED) + _multiwidth_index(
                [(bytes(cid.raw_digest), off) for cid, off in entries]
            )
        else:
            by_code = {}
            for cid, off in entries:
                # like go-car, the keys are the raw digests without the
                # multihash prefix
                by_code.setdefault(cid.hashfun.code, []).append(
                    (bytes(cid.raw_digest), off)
                )
            index_bytes = varint.encode(idx.MULTIHASH_INDEX_SORTED)
            index_bytes += struct.pack("<i", len(by_code))
            for code, code_entries in sorted(by_code.items()):
                index_bytes += struct.pack("<Q", code) + _multiwidth_index(code_entries)
    characteristics = bytes([0x80 if index else 0]) + bytes(15)
    header = characteristics + struct.pack("<QQQ", data_offset, len(data), index_offset)
    out = idx.CARV2_PRAGMA + header + bytes(padding) + data
    if index is not None:
        out += bytes(padding) + index_bytes
    return out


@pytest.fixture
def dag():
    a = raw_block(b"first chunk|")
    b = raw_block(b"second chunk|")
    c = raw_block(b"third and last chunk")
    chunked = file_node([a, b, a, c])
    single = file_node([b])
    root = directory_node({"chunked": chunked, "single": single})
    blocks = [root, chunked, single, a, b, c]
    expected = {"chunked": a[1] + b[1] + a[1] + c[1], "single": b[1]}
    return root[0], blocks, expected


@pytest.mark.parametrize("padding", [0, 7])
@pytest.mark.parametrize("index", ["multihash", "sorted", None])
def test_carv2_index_matches_carv1(tmp_path, dag, index, padding):
    root, blocks, _ = dag
    v1 = tmp_path / "test.v1.car"
    v2 = tmp_path / "test.v2.car"
    write_carv1(v1, [root], blocks)
    v2.write_bytes(carv2_bytes([root], blocks, index=index, padding=padding))

    assert not idx.is_carv2(v1)
    assert idx.is_carv2(v2)

    entries_v1 = list(idx.read_car(str(v1)))
    entries_v2 = list(idx.read_car(str(v2)))
    assert len(entries_v2) == len(blocks)

    # the payload is shifted by the CARv2 header, but the content is the same
    content = v2.read_bytes()
    for (cid1, _, _, size1), (cid2, fn, off, size2) in zip(entries_v1, entries_v2):
        assert cid1 == cid2
        assert size1 == size2
        assert fn == str(v2)
    by_cid = {cid.digest: block for cid, block in blocks}
    for cid, _, off, size in entries_v2:
        assert content[off : off + size] == by_cid[cid]


@pytest.mark.parametrize("index", ["multihash", "sorted", None])
def test_carv2_preffs(tmp_path, dag, index):
    root, blocks, expected = dag
    car = tmp_path / "test.car"
    car.write_bytes(carv2_bytes([root], blocks, index=index, padding=3))
    index_df = idx.generate_index([str(car)])
    parquet_fn = str(tmp_path / "preffs.parquet")
    ref.create_preffs(root, index_df, parquet_fn=parquet_fn)

    m = fsspec.get_mapper(f"preffs::{parquet_fn}")
    assert sorted(m.keys()) == sorted(expected.keys())
    for key, content in expected.items():
        assert m[key] == content


def test_mixed_car_versions(tmp_path, dag):
    root, blocks, expected = dag
    v1 = tmp_path / "test.0.car"
    v2 = tmp_path / "test.1.car"
    write_carv1(v1, [root], blocks[:3])
    v2.write_bytes(carv2_bytes([root], blocks[3:]))
    index_df = idx.generate_index([str(v1), str(v2)])
    preffs = ref.create_preffs(root, index_df)
    assert set(preffs.index) == set(expected)


@pytest.mark.parametrize("index", ["multihash", "sorted"])
def test_carv2_index_is_used(dag, index):
    root, blocks, _ = dag
    content = carv2_bytes([root], blocks, index=index)
    _, _, index_offset = struct.unpack_from("<QQQ", content, 11 + 16)
    offsets = idx._read_carv2_index(content, index_offset)
    assert offsets is not None
    assert len(offsets) == len(blocks)


def test_carv2_unknown_index_falls_back_to_scan(tmp_path, dag):
    root, blocks, expected = dag
    content = bytearray(carv2_bytes([root], blocks, index="sorted"))
    _, _, index_offset = struct.unpack_from("<QQQ", content, 11 + 16)
    content[index_offset : index_offset + 2] = varint.encode(0x0402)  # unknown
    car = tmp_path / "test.car"
    car.write_bytes(bytes(content))
    assert len(list(idx.read_car(str(car)))) == len(blocks)
