"""Tests for the fsspec filesystem for indexed CARv2 files."""

import fsspec
import pytest
from ipldstore.unixfsv1 import Data, DataType, PBLink, PBNode
from multiformats import CID, multihash

from car_referencer.carfs import CARFileSystem

from .car_utils import SHA256, directory_node, file_node, raw_block
from .test_carv2 import carv2_bytes


def inline_file(payload):
    """dag-pb file node with the data inlined in the node."""
    data = Data(Type=DataType.File, Data=payload, filesize=len(payload)).dumps()
    block = PBNode(Links=[], Data=data).dumps()
    return CID("base32", 1, "dag-pb", SHA256.digest(block)), block


def nested_file(children):
    """File node linking to ``(cid, block, filesize)`` children."""
    data = Data(
        Type=DataType.File,
        filesize=sum(size for _, _, size in children),
        blocksizes=[size for _, _, size in children],
    ).dumps()
    links = [PBLink(Hash=bytes(cid), Name="", Tsize=len(b)) for cid, b, _ in children]
    block = PBNode(Links=links, Data=data).dumps()
    return CID("base32", 1, "dag-pb", SHA256.digest(block)), block


def identity_block(payload):
    cid = CID("base32", 1, "raw", multihash.get("identity").digest(payload))
    return cid, payload


def hamt_shard(entries, fanout=256):
    """Minimal single-level HAMT shard (bucket prefixes are not hashed)."""
    data = Data(Type=DataType.HAMTShard, fanout=fanout, hashType=0x22).dumps()
    links = [
        PBLink(Hash=bytes(cid), Name=f"{i:02X}{name}", Tsize=len(block))
        for i, (name, (cid, block)) in enumerate(sorted(entries.items()))
    ]
    block = PBNode(Links=links, Data=data).dumps()
    return CID("base32", 1, "dag-pb", SHA256.digest(block)), block


@pytest.fixture
def dag():
    a = raw_block(b"first chunk|")
    b = raw_block(b"second chunk|")
    c = raw_block(b"third and last chunk")
    chunked = file_node([a, b, a, c])
    # two levels of file nodes
    nested = nested_file([(*chunked, 57), (*b, len(b[1]))])
    single = file_node([b])
    inline = inline_file(b"inlined data")
    ident = identity_block(b"tiny")
    sub = directory_node({"nested": nested, "inline": inline})
    shard = hamt_shard({"x": a, "y": c})
    root = directory_node(
        {
            "chunked": chunked,
            "single": single,
            "raw": c,
            "ident": ident,
            "sub": sub,
            "shard": shard,
        }
    )
    # identity blocks are not stored in the CAR file
    blocks = [root, chunked, nested, single, inline, sub, shard, a, b, c]
    contents = {
        "chunked": a[1] + b[1] + a[1] + c[1],
        "single": b[1],
        "raw": c[1],
        "ident": b"tiny",
        "sub/nested": a[1] + b[1] + a[1] + c[1] + b[1],
        "sub/inline": b"inlined data",
        "shard/x": a[1],
        "shard/y": c[1],
    }
    return root[0], blocks, contents, sub[0]


@pytest.fixture(params=["multihash", "sorted"])
def car(tmp_path, dag, request):
    root, blocks, *_ = dag
    fn = tmp_path / "test.car"
    fn.write_bytes(carv2_bytes([root], blocks, index=request.param, padding=5))
    return str(fn)


def test_listing(car, dag):
    fs = CARFileSystem(car)
    assert sorted(fs.ls("/", detail=False)) == [
        "chunked",
        "ident",
        "raw",
        "shard",
        "single",
        "sub",
    ]
    assert sorted(fs.ls("sub", detail=False)) == ["sub/inline", "sub/nested"]
    assert sorted(fs.ls("shard", detail=False)) == ["shard/x", "shard/y"]
    assert fs.isdir("sub")
    assert fs.isfile("sub/nested")
    assert not fs.exists("missing")
    assert sorted(fs.find("/")) == sorted(dag[2])


def test_content(car, dag):
    fs = CARFileSystem(car)
    for path, content in dag[2].items():
        assert fs.size(path) == len(content), path
        assert fs.cat(path) == content, path
        with fs.open(path) as f:
            assert f.read() == content


def test_ranges(car, dag):
    fs = CARFileSystem(car)
    content = dag[2]["sub/nested"]
    for start in range(len(content)):
        for end in range(start, len(content) + 2, 3):
            assert fs.cat_file("sub/nested", start, end) == content[start:end]
    assert fs.cat_file("sub/nested", -5) == content[-5:]
    with fs.open("sub/nested", block_size=7) as f:
        f.seek(10)
        assert f.read(20) == content[10:30]


def test_fsspec_url(car, dag):
    with fsspec.open(f"car://sub/nested::{car}") as f:
        assert f.read() == dag[2]["sub/nested"]
    m = fsspec.get_mapper("car://", fo=car)
    assert m["shard/y"] == dag[2]["shard/y"]


def test_explicit_root(car, dag):
    _, _, contents, sub = dag
    assert CID.decode(CARFileSystem(car).info("sub")["cid"]) == sub
    fs = CARFileSystem(car, root=str(sub))
    assert fs.cat("inline") == contents["sub/inline"]


def test_multiple_files(tmp_path, dag):
    root, blocks, contents, _ = dag
    files = [tmp_path / "a.car", tmp_path / "b.car"]
    files[0].write_bytes(carv2_bytes([root], blocks[:5]))
    files[1].write_bytes(carv2_bytes([root], blocks[5:]))
    fs = CARFileSystem([str(f) for f in files])
    for path, content in contents.items():
        assert fs.cat(path) == content


def test_missing_block(tmp_path, dag):
    root, blocks, *_ = dag
    fn = tmp_path / "test.car"
    fn.write_bytes(carv2_bytes([root], blocks[:-1]))  # last raw chunk is missing
    fs = CARFileSystem(str(fn))
    with pytest.raises(FileNotFoundError):
        fs.cat("raw")


def test_requires_index(tmp_path, dag):
    root, blocks, *_ = dag
    fn = tmp_path / "test.car"
    fn.write_bytes(carv2_bytes([root], blocks, index=None))
    with pytest.raises(ValueError, match="index"):
        CARFileSystem(str(fn))


def test_requires_carv2():
    with pytest.raises(ValueError, match="CARv2"):
        CARFileSystem("tests/test.car")


def test_remote_reads_are_ranged(tmp_path, car, dag):
    """Only header, index and the touched blocks are read."""
    calls = []
    local = fsspec.filesystem("file")

    class Recording(type(local)):
        def cat_file(self, path, start=None, end=None, **kwargs):
            calls.append((start, end))
            return super().cat_file(path, start, end, **kwargs)

    fs = CARFileSystem(car, fs=Recording(skip_instance_cache=True))
    fs.cat("single")
    assert all(start is not None for start, _ in calls)
    size = local.size(car)
    assert sum((end or size) - start for start, end in calls) < size
