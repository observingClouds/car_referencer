"""fsspec filesystem for indexed CARv2 files.

The filesystem exposes the UnixFS DAG contained in one or more indexed CARv2
files as a read-only filesystem. Only the fixed CARv2 header and the index
are read when the filesystem is created. Directory and file nodes are loaded
on demand and file content is read with ranged requests directly from the
CAR files, so the underlying files can live on any fsspec filesystem
(local, http, s3, ...).

Example
-------

>>> import fsspec
>>> fs = fsspec.filesystem("car", fo="s3://bucket/example.car")
>>> fs.ls("/")
>>> import xarray as xr
>>> ds = xr.open_zarr("car://example.zarr::s3://bucket/example.car")
"""

import struct
from functools import lru_cache

import dag_cbor
from fsspec.core import url_to_fs
from fsspec.registry import filesystem
from fsspec.spec import AbstractBufferedFile, AbstractFileSystem
from ipldstore.unixfsv1 import Data, DataType, PBNode
from multiformats import CID, multihash

from .index import (
    CARV2_HEADER_SIZE,
    CARV2_PRAGMA,
    INDEX_SORTED,
    MULTIHASH_INDEX_SORTED,
    _cid_end,
    _uvarint,
)

# A section header consists of a varint (<= 10 bytes) and a CID. CIDs with
# digests of up to 64 bytes fit easily into this many bytes.
SECTION_HEADER_READ_SIZE = 128


def _parse_multiwidth_index(buf, pos, code, out):
    """Parse a go-car ``IndexSorted`` body into ``out``.

    ``out`` maps ``(multihash code or None, raw digest)`` to the offset of the
    section relative to the data payload. Returns the position after the body.
    """
    (n_buckets,) = struct.unpack_from("<i", buf, pos)
    pos += 4
    for _ in range(n_buckets):
        width, length = struct.unpack_from("<Iq", buf, pos)
        pos += 12
        for entry in range(pos, pos + length, width):
            digest = bytes(buf[entry : entry + width - 8])
            (offset,) = struct.unpack_from("<Q", buf, entry + width - 8)
            out[(code, digest)] = offset
        pos += length
    return pos


def parse_carv2_index(buf):
    """Parse a CARv2 index into ``{(code, raw digest): section offset}``.

    For ``IndexSorted`` indices the multihash code is unknown and ``None``.
    """
    codec, pos = _uvarint(buf, 0)
    out = {}
    if codec == INDEX_SORTED:
        _parse_multiwidth_index(buf, pos, None, out)
    elif codec == MULTIHASH_INDEX_SORTED:
        (n_codes,) = struct.unpack_from("<i", buf, pos)
        pos += 4
        for _ in range(n_codes):
            (code,) = struct.unpack_from("<Q", buf, pos)
            pos = _parse_multiwidth_index(buf, pos + 8, code, out)
    else:
        raise NotImplementedError(f"CARv2 index format 0x{codec:x} is not supported")
    return out


class _Car:
    """A single indexed CARv2 file on some fsspec filesystem."""

    def __init__(self, fs, path):
        self.fs = fs
        self.path = path
        header = fs.cat_file(path, 0, len(CARV2_PRAGMA) + CARV2_HEADER_SIZE)
        if header[: len(CARV2_PRAGMA)] != CARV2_PRAGMA:
            raise ValueError(f"{path} is not a CARv2 file")
        self.data_offset, self.data_size, index_offset = struct.unpack_from(
            "<QQQ", header, len(CARV2_PRAGMA) + 16
        )
        if index_offset == 0:
            raise ValueError(
                f"{path} does not contain an index. Use e.g. "
                "`car index` from go-car to add one."
            )
        self.index = parse_carv2_index(fs.cat_file(path, index_offset, None))

        # the inner CARv1 header contains the roots
        head = fs.cat_file(path, self.data_offset, self.data_offset + 16)
        size, pos = _uvarint(head, 0)
        start = self.data_offset + pos
        car_header = dag_cbor.decode(fs.cat_file(path, start, start + size))
        self.roots = list(car_header.get("roots", []))

    def lookup(self, cid):
        """Return the section offset (relative to the data payload) of ``cid``
        or ``None`` if it is not indexed in this file."""
        raw = bytes(cid.raw_digest)
        code = cid.hashfun.code
        offset = self.index.get((code, raw))
        if offset is None:
            offset = self.index.get((None, raw))
        return offset

    def locate(self, offset):
        """Return ``(cid, absolute block offset, block size)`` of the section at
        ``offset`` relative to the data payload."""
        start = self.data_offset + offset
        end = min(start + SECTION_HEADER_READ_SIZE, self.data_offset + self.data_size)
        buf = self.fs.cat_file(self.path, start, end)
        length, cid_start = _uvarint(buf, 0)
        cid_end = _cid_end(buf, cid_start)
        cid = CID.decode(bytes(buf[cid_start:cid_end]))
        return cid, start + cid_end, cid_start + length - cid_end


class CARFileSystem(AbstractFileSystem):
    """Read-only filesystem for UnixFS DAGs stored in indexed CARv2 files.

    Parameters
    ----------
    fo : str or list of str
        Path(s) or URL(s) of the CARv2 file(s). Blocks of the DAG can be
        distributed over several files.
    root : str or CID, optional
        CID of the UnixFS node that forms the root of the filesystem. Defaults
        to the first root listed in the header of the first CAR file.
    target_protocol : str, optional
        Protocol of the filesystem the CAR files are stored on. Inferred from
        ``fo`` if not given.
    target_options : dict, optional
        Options for the filesystem the CAR files are stored on.
    fs : AbstractFileSystem, optional
        Filesystem instance the CAR files are stored on. If given,
        ``target_protocol`` and ``target_options`` are ignored.
    """

    protocol = "car"
    root_marker = ""

    def __init__(
        self,
        fo,
        root=None,
        target_protocol=None,
        target_options=None,
        fs=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        target_options = target_options or {}
        self.cars = []
        for path in [fo] if isinstance(fo, str) else list(fo):
            if fs is not None:
                car_fs = fs
            elif target_protocol is not None:
                car_fs = filesystem(target_protocol, **target_options)
                path = car_fs._strip_protocol(path)
            else:
                car_fs, path = url_to_fs(path, **target_options)
            self.cars.append(_Car(car_fs, path))

        if root is None:
            roots = self.cars[0].roots
            if not roots:
                raise ValueError("no root given and the CAR file has no roots")
            root = roots[0]
        self.root = root if isinstance(root, CID) else CID.decode(root)
        self._locate = lru_cache(maxsize=None)(self._locate_uncached)
        self._node = lru_cache(maxsize=1024)(self._node_uncached)
        self._directory = lru_cache(maxsize=256)(self._directory_uncached)

    @classmethod
    def _strip_protocol(cls, path):
        path = super()._strip_protocol(path)
        return path.strip("/")

    # --- block access -----------------------------------------------------

    def _locate_uncached(self, cid):
        """Return ``(car, absolute offset, size)`` of the block ``cid``."""
        for car in self.cars:
            offset = car.lookup(cid)
            if offset is not None:
                found, start, size = car.locate(offset)
                if found.digest == cid.digest:
                    return car, start, size
        raise FileNotFoundError(f"block {cid} is not contained in the CAR file(s)")

    def _block(self, cid):
        if _is_identity(cid):
            return bytes(cid.raw_digest)
        car, start, size = self._locate(cid)
        return car.fs.cat_file(car.path, start, start + size)

    def _node_uncached(self, cid):
        """Load the dag-pb node ``cid``.

        Returns the node, its UnixFS data and the decoded CIDs of its links.
        """
        node = PBNode.loads(self._block(cid))
        links = [CID.decode(link.Hash) for link in node.Links]
        return node, Data.loads(node.Data), links

    # --- UnixFS -----------------------------------------------------------

    def _entry(self, name, cid):
        """Return the fsspec info dict of the UnixFS node ``cid``."""
        if cid.codec.name == "raw":
            if _is_identity(cid):
                size = len(cid.raw_digest)
            else:
                size = self._locate(cid)[2]
            return {"name": name, "type": "file", "size": size, "cid": str(cid)}
        if cid.codec.name != "dag-pb":
            raise NotImplementedError(f"codec {cid.codec.name} is not supported")
        node, data, links = self._node(cid)
        if data.Type in (DataType.Directory, DataType.HAMTShard):
            return {"name": name, "type": "directory", "size": 0, "cid": str(cid)}
        if data.Type in (DataType.File, DataType.Raw):
            return {
                "name": name,
                "type": "file",
                "size": self._file_size(cid),
                "cid": str(cid),
            }
        raise NotImplementedError(f"UnixFS type {data.Type} is not supported")

    def _directory_uncached(self, cid):
        """Return ``{name: cid}`` of the entries of the directory ``cid``."""
        return dict(self._children(cid))

    def _children(self, cid):
        """Yield ``(name, cid)`` of the entries of the directory ``cid``."""
        node, data, links = self._node(cid)
        if data.Type == DataType.Directory:
            for link, child in zip(node.Links, links):
                yield link.Name, child
        elif data.Type == DataType.HAMTShard:
            # link names are prefixed with the hex encoded bucket index. Links
            # without a name besides the prefix point to sub-shards.
            prefix = len(f"{data.fanout - 1:X}")
            for link, child in zip(node.Links, links):
                if len(link.Name) > prefix:
                    yield link.Name[prefix:], child
                else:
                    yield from self._children(child)
        else:
            raise NotADirectoryError(str(cid))

    def _resolve(self, path):
        """Return the CID of the node at ``path``."""
        cid = self.root
        for part in filter(None, path.split("/")):
            if cid.codec.name != "dag-pb":
                raise NotADirectoryError(path)
            try:
                cid = self._directory(cid)[part]
            except (KeyError, NotADirectoryError):
                raise FileNotFoundError(path) from None
        return cid

    def _file_size(self, cid):
        if _is_identity(cid):
            return len(cid.raw_digest)
        if cid.codec.name == "raw":
            return self._locate(cid)[2]
        node, data, links = self._node(cid)
        if data.filesize is not None:
            return data.filesize
        return len(data.Data or b"") + sum(self._child_sizes(cid))

    def _child_sizes(self, cid):
        node, data, links = self._node(cid)
        if len(data.blocksizes) == len(node.Links):
            return list(data.blocksizes)
        return [self._file_size(child) for child in links]

    def _pieces(self, cid, start, end):
        """Yield the content of the file node ``cid`` between ``start`` and
        ``end`` as ``bytes`` or as ``(car, start, end)`` byte ranges."""
        if start >= end:
            return
        if _is_identity(cid):
            yield bytes(cid.raw_digest[start:end])
            return
        if cid.codec.name == "raw":
            car, offset, size = self._locate(cid)
            if start < size:
                yield car, offset + start, offset + min(end, size)
            return
        node, data, links = self._node(cid)
        inline = data.Data or b""
        if start < len(inline):
            yield bytes(inline[start:end])
        pos = len(inline)
        for child, size in zip(links, self._child_sizes(cid)):
            if pos >= end:
                break
            if pos + size > start:
                yield from self._pieces(
                    child, max(start - pos, 0), min(end - pos, size)
                )
            pos += size

    def _read(self, path, start, end):
        info = self.info(path)
        if info["type"] != "file":
            raise IsADirectoryError(path)
        size = info["size"]
        start = 0 if start is None else start
        end = size if end is None else end
        if start < 0:
            start = max(size + start, 0)
        if end < 0:
            end = size + end
        end = min(end, size)
        pieces = list(self._pieces(CID.decode(info["cid"]), start, end))
        ranges = [p for p in pieces if not isinstance(p, bytes)]
        # fetch all byte ranges in one go, which is more efficient for remote
        # filesystems that support concurrent requests
        fetched = iter([])
        if ranges:
            by_fs = {}
            for i, (car, s, e) in enumerate(ranges):
                by_fs.setdefault(id(car.fs), (car.fs, []))[1].append((i, car.path, s, e))
            results = [None] * len(ranges)
            for fs, items in by_fs.values():
                data = fs.cat_ranges(
                    [p for _, p, _, _ in items],
                    [s for _, _, s, _ in items],
                    [e for _, _, _, e in items],
                )
                for (i, _, _, _), d in zip(items, data):
                    if isinstance(d, Exception):
                        raise d
                    results[i] = d
            fetched = iter(results)
        return b"".join(p if isinstance(p, bytes) else next(fetched) for p in pieces)

    # --- fsspec API -------------------------------------------------------

    def ls(self, path, detail=True, **kwargs):
        path = self._strip_protocol(path)
        if path in self.dircache:
            entries = self.dircache[path]
        else:
            cid = self._resolve(path)
            info = self._entry(path, cid)
            if info["type"] == "file":
                entries = [info]
            else:
                entries = [
                    self._entry(f"{path}/{name}" if path else name, child)
                    for name, child in self._directory(cid).items()
                ]
                self.dircache[path] = entries
        return entries if detail else [e["name"] for e in entries]

    def info(self, path, **kwargs):
        path = self._strip_protocol(path)
        parent = self._parent(path)
        if path and parent in self.dircache:
            for entry in self.dircache[parent]:
                if entry["name"] == path:
                    return entry
            raise FileNotFoundError(path)
        return self._entry(path, self._resolve(path))

    def cat_file(self, path, start=None, end=None, **kwargs):
        return self._read(self._strip_protocol(path), start, end)

    def _open(self, path, mode="rb", block_size=None, autocommit=True, **kwargs):
        if mode != "rb":
            raise NotImplementedError("CARFileSystem is read-only")
        path = self._strip_protocol(path)
        info = self.info(path)
        if info["type"] != "file":
            raise IsADirectoryError(path)
        return CARFile(self, path, mode, block_size, size=info["size"], **kwargs)


class CARFile(AbstractBufferedFile):
    def _fetch_range(self, start, end):
        return self.fs._read(self.path, start, end)


def _is_identity(cid):
    return cid.hashfun.code == multihash.get("identity").code
