import mmap
from functools import lru_cache

import pandas as pd
from ipldstore.unixfsv1 import Data, DataType, PBNode
from multiformats import CID


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
                yield "/".join(path), None, None, None, data.Data
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
    ref_fs = gen_refs(index, root_cid)
    df_preffs = pd.DataFrame.from_records(
        ref_fs, columns=["key", "path", "offset", "size", "raw"], index="key"
    )
    df_preffs = df_preffs.sort_index(kind="stable")
    if parquet_fn:
        df_preffs.to_parquet(parquet_fn)
    return df_preffs
