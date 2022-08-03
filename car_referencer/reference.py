import base64

import pandas as pd
from ipldstore.unixfsv1 import Data, PBNode
from multiformats import CID


def load_data(data):
    node = PBNode.loads(data)
    data = Data.loads(node.Data)
    return node, data


def create_reference_fs(root_cid, index):
    entry = index.loc[root_cid.digest]
    with open(entry.file, "rb") as f:
        f.seek(entry["offset"])
        data = f.read(entry["size"])
        node, data = load_data(data)
    return node, data


def list_links(node):
    d = [[link.Name, CID.decode(link.Hash)] for link in node.Links]
    return d


def get_size(blocksizes, n, entry):
    if blocksizes != []:
        offset = entry.offset + sum(blocksizes[0:n])
        size = blocksizes[n]
    else:
        offset = entry.offset
        size = entry["size"]
    return offset, size


def loop_create_reffs(cid, index, ref_fs=[], dir=None):
    node, data = create_reference_fs(cid, index)
    for n, (name, hash) in enumerate(list_links(node)):
        if hash.hashfun.code == 18:  # raw
            if hash.codec.name == "dag-pb":
                ref_fs = loop_create_reffs(
                    hash, index, ref_fs, dir="/".join(filter(bool, [dir, name]))
                )
            entry = index.loc[hash.digest]
            if isinstance(entry, pd.DataFrame):
                entry = entry.sort_values("order")
                ref_fs.extend(
                    [
                        [
                            "/".join(filter(bool, [dir, name])),
                            e.file,
                            # e.offset,
                            # e["size"],
                            get_size(data.blocksizes, n, e)[0],
                            get_size(data.blocksizes, n, e)[1],
                            None,
                        ]
                        for _, e in entry.iterrows()
                    ]
                )
            else:  # single entry per zarr-file
                ref_fs.append(
                    [
                        "/".join(filter(bool, [dir, name])),
                        entry.file,
                        # entry.offset,
                        # entry["size"],
                        get_size(data.blocksizes, n, entry)[0],
                        get_size(data.blocksizes, n, entry)[1],
                        None,
                    ]
                )
        elif hash.hashfun.code == 0:  # identity
            ref_fs.append(
                [
                    "/".join(filter(bool, [dir, name])),
                    None,
                    None,
                    None,
                    base64.b64decode(base64.b64encode(hash.raw_digest).decode("ascii")),
                ]
            )
        else:
            raise ValueError(
                f"Found unknown hash code {hash.hashfun.code}, {name}, {hash.codec.name}"
            )
    return ref_fs


def create_preffs(cid, index, parquet_fn=None):
    ref_fs = loop_create_reffs(cid, index)
    df_preffs = pd.DataFrame.from_records(
        ref_fs, columns=["key", "path", "offset", "size", "raw"], index="key"
    )
    df_preffs = df_preffs.sort_index(kind="stable")
    df_preffs = df_preffs.convert_dtypes()
    if parquet_fn:
        df_preffs.to_parquet(parquet_fn)
    return df_preffs
