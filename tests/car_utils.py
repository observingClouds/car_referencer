"""Helpers to build small UnixFS DAGs and write them into CAR files."""

import dag_cbor
from ipldstore.unixfsv1 import Data, DataType, PBLink, PBNode
from multiformats import CID, multihash, varint

SHA256 = multihash.get("sha2-256")


def raw_block(payload):
    """Return ``(cid, payload)`` of a raw leaf block."""
    return CID("base32", 1, "raw", SHA256.digest(payload)), payload


def file_node(chunks):
    """Return ``(cid, block)`` of a UnixFS file node linking to ``chunks``.

    ``chunks`` is a list of ``(cid, payload)`` tuples as returned by
    :func:`raw_block`. The same chunk may appear multiple times.
    """
    data = Data(
        Type=DataType.File,
        filesize=sum(len(p) for _, p in chunks),
        blocksizes=[len(p) for _, p in chunks],
    ).dumps()
    links = [PBLink(Hash=bytes(cid), Name="", Tsize=len(p)) for cid, p in chunks]
    block = PBNode(Links=links, Data=data).dumps()
    return CID("base32", 1, "dag-pb", SHA256.digest(block)), block


def directory_node(entries):
    """Return ``(cid, block)`` of a UnixFS directory.

    ``entries`` maps names to ``(cid, block)`` tuples.
    """
    data = Data(Type=DataType.Directory).dumps()
    links = [
        PBLink(Hash=bytes(cid), Name=name, Tsize=len(block))
        for name, (cid, block) in sorted(entries.items())
    ]
    block = PBNode(Links=links, Data=data).dumps()
    return CID("base32", 1, "dag-pb", SHA256.digest(block)), block


def carv1_bytes(roots, blocks):
    """Serialize ``blocks`` (list of ``(cid, block)``) as CARv1."""
    header = dag_cbor.encode({"version": 1, "roots": list(roots)})
    out = bytearray(varint.encode(len(header)) + header)
    for cid, block in blocks:
        section = bytes(cid) + block
        out += varint.encode(len(section)) + section
    return bytes(out)


def write_carv1(filename, roots, blocks):
    with open(filename, "wb") as f:
        f.write(carv1_bytes(roots, blocks))


def assert_matches_ipldstore(carfile):
    """Check car_referencer's CARv1 reader and UnixFS decoder against
    ipldstore for all blocks in ``carfile``. Returns the number of decoded
    dag-pb blocks."""
    import ipldstore
    from ipldstore import unixfsv1

    import car_referencer.index as idx
    from car_referencer import unixfs

    with open(carfile, "rb") as f:
        _, blocks = ipldstore.car.read_car(f)
        expected = [
            (cid, data, loc.payload_offset, loc.payload_size)
            for cid, data, loc in blocks
        ]
    actual = list(idx.read_carv1(str(carfile), verify=True))
    assert [(cid.digest, off, size) for cid, _, off, size in expected] == [
        (digest, off, size) for digest, _, off, size in actual
    ]

    n_dag_pb = 0
    for cid, data, _, _ in expected:
        if cid.codec.name != "dag-pb":
            continue
        n_dag_pb += 1
        node = unixfs.PBNode.loads(data)
        node_ref = unixfsv1.PBNode.loads(data)
        assert node.Data == node_ref.Data
        assert [(lk.Hash, lk.Name, lk.Tsize) for lk in node.Links] == [
            (lk.Hash, lk.Name, lk.Tsize) for lk in node_ref.Links
        ]
        meta = unixfs.Data.loads(node.Data)
        meta_ref = unixfsv1.Data.loads(node_ref.Data)
        assert int(meta.Type) == int(meta_ref.Type)
        assert meta.Data == meta_ref.Data
        assert meta.filesize == meta_ref.filesize
        assert list(meta.blocksizes) == list(meta_ref.blocksizes)
    return n_dag_pb
