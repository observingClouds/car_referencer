import base64
import json
import mmap
from functools import lru_cache

import pandas as pd
from multiformats import CID

from .unixfs import Data, DataType, PBNode


def load_dag_pb(data):
    node = PBNode.loads(data)
    data = Data.loads(node.Data)
    return node, data


def is_identity(cid):
    return cid.hashfun.codec.name == "identity"


def find_cid(index, cid):
    entry = index.loc[cid.digest]
    return entry["file"], entry["offset"], entry["size"]


@lru_cache(128)
def ro_mapped_file(filename):
    fh = open(filename, "rb")
    return mmap.mmap(fh.fileno(), 0, prot=mmap.PROT_READ)


def get_content(index, cid):
    if is_identity(cid):
        return cid.raw_digest
    else:
        file, offset, size = find_cid(index, cid)
        return ro_mapped_file(file)[offset : offset + size]


def gen_refs(index, root, path=None):
    path = path or []
    if root.codec.name == "dag-pb":
        node, data = load_dag_pb(get_content(index, root))
        if data.Type == DataType.Directory:
            for link in node.Links:
                yield from gen_refs(index, CID.decode(link.Hash), path + [link.Name])
        elif data.Type == DataType.File:
            if len(node.Links) == 0:  # data inlined in file object
                yield "/".join(path), None, None, None, data.Data or b""
            else:  # data is chunked and linked
                for link in node.Links:
                    yield from gen_refs(index, CID.decode(link.Hash), path)
        else:
            raise NotImplementedError(f"{data.Type}")
    elif (
        root.codec.name == "raw"
    ):  # raw data can be accessed via direct pointers into car file
        if is_identity(root):
            yield "/".join(path), None, None, None, root.raw_digest
        else:
            file, offset, size = find_cid(index, root)
            yield "/".join(path), file, offset, size, None
    else:
        raise NotImplementedError(f"{root.codec.name}")


def create_preffs(root_cid, index, parquet_fn=None):
    """Create a preffs reference filesystem.

    Parameters
    ----------
    root_cid : multiformats.CID
        CID of the root object (e.g. the zarr directory)
    index : pandas.DataFrame
        Index of the car files as created by
        :func:`car_referencer.index.generate_index`
    parquet_fn : str, optional
        Filename of the parquet file to be written

    Returns
    -------
    pandas.DataFrame
        References with one row per (chunk of a) file. Files that are split
        into several chunks have several rows with the same key.
    """
    ref_fs = gen_refs(index, root_cid)
    df_preffs = pd.DataFrame.from_records(
        ref_fs, columns=["key", "path", "offset", "size", "raw"], index="key"
    )
    df_preffs = df_preffs.sort_index(kind="stable")
    if parquet_fn:
        df_preffs.to_parquet(parquet_fn)
    return df_preffs


def _inline(data):
    """Encode inline data as in the kerchunk/fsspec reference spec."""
    data = bytes(data)
    try:
        text = data.decode("ascii")
    except UnicodeDecodeError:
        pass
    else:
        if not text.startswith("base64:"):
            return text
    return "base64:" + base64.b64encode(data).decode("ascii")


def create_references(root_cid, index, json_fn=None, inline_chunked=False):
    """Create a kerchunk/fsspec reference filesystem (version 1).

    The references can be opened with ``fsspec``'s ``ReferenceFileSystem``,
    e.g. ``fsspec.get_mapper("reference://", fo=json_fn)``.

    In this format every file maps to a single byte range. Files that are
    split into several chunks in the car files can therefore only be added
    by copying their content into the reference file (``inline_chunked``).
    Use :func:`create_preffs` to reference such files without copying.

    Parameters
    ----------
    root_cid : multiformats.CID
        CID of the root object (e.g. the zarr directory)
    index : pandas.DataFrame
        Index of the car files as created by
        :func:`car_referencer.index.generate_index`
    json_fn : str, optional
        Filename of the JSON file to be written
    inline_chunked : bool, default False
        Copy the content of files that consist of several chunks into the
        reference file. If False, a ValueError is raised for such files.

    Returns
    -------
    dict
        References as ``{"version": 1, "refs": {...}}``
    """
    entries = {}
    for key, path, offset, size, raw in gen_refs(index, root_cid):
        entries.setdefault(key, []).append((path, offset, size, raw))

    refs = {}
    for key, parts in sorted(entries.items()):
        if len(parts) == 1:
            path, offset, size, raw = parts[0]
            if raw is not None:
                refs[key] = _inline(raw)
            else:
                refs[key] = [str(path), int(offset), int(size)]
        elif inline_chunked:
            refs[key] = _inline(
                b"".join(
                    (
                        raw
                        if raw is not None
                        else ro_mapped_file(path)[offset : offset + size]
                    )
                    for path, offset, size, raw in parts
                )
            )
        else:
            raise ValueError(
                f"'{key}' consists of {len(parts)} chunks, which cannot be"
                " referenced as a single byte range. Use the preffs format or"
                " inline the content (inline_chunked=True)."
            )

    references = {"version": 1, "refs": refs}
    if json_fn:
        with open(json_fn, "w") as f:
            json.dump(references, f)
    return references
