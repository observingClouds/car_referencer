import base64

import h5py
import numpy as np
import pandas as pd
import tables
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
    d = {link.Name: CID.decode(link.Hash) for link in node.Links}
    return d


class Reference(tables.IsDescription):
    key = tables.StringCol(1000)
    path = tables.StringCol(1000)
    offset = tables.Float64Col(dflt=-1)
    size = tables.Float64Col(dflt=-1)
    raw = tables.StringCol(1000)


def loop_create_reffs(cid, index, table, dir=None):
    reference = table.row
    node, data = create_reference_fs(cid, index)
    for n, (name, hash) in enumerate(list_links(node).items()):
        if (
            name is None
        ):  # are these fake cids? why do I find those although I'm using the actual root CID
            continue
        if hash.hashfun.code == 18:  # raw
            if hash.codec.name == "dag-pb":
                loop_create_reffs(
                    hash, index, table, dir="/".join(filter(bool, [dir, name]))
                )
            entry = index.loc[hash.digest]
            if isinstance(entry, pd.DataFrame):
                entry = entry.sort_values("order")
                for _, e in entry.iterrows():
                    reference["key"] = "/".join(filter(bool, [dir, name]))
                    reference["path"] = e.file
                    reference["offset"] = e.offset
                    reference["size"] = e["size"]
                    # reference["raw"] = None
                    reference.append()
            else:  # single entry per zarr-file
                reference["key"] = "/".join(filter(bool, [dir, name]))
                reference["path"] = entry.file
                reference["offset"] = entry.offset
                reference["size"] = entry["size"]
                # reference["raw"] = None
                reference.append()
        elif hash.hashfun.code == 0:  # identity
            reference["key"] = "/".join(filter(bool, [dir, name]))
            # reference["path"] = None
            # reference["offset"] = None
            # reference["size"] = None
            reference["raw"] = base64.b64decode(
                base64.b64encode(hash.raw_digest).decode("ascii")
            )
            reference.append()
            print(
                len(base64.b64decode(base64.b64encode(hash.raw_digest).decode("ascii")))
            )
    if n > 10000:
        table.flush()
    return


def create_preffs(cid, index, parquet_fn=None):
    h5file = tables.open_file(
        "car_references.h5.partial", mode="w", title="Car references"
    )
    table = h5file.create_table("/", "references", Reference, "Readout example")

    loop_create_reffs(cid, index, table)
    table.flush()
    h5 = h5py.File("car_references.h5.partial")

    df_preffs = pd.DataFrame.from_records(
        h5["references"], columns=["key", "offset", "path", "raw", "size"], index="key"
    )
    df_preffs = df_preffs.sort_index(kind="stable")
    # df_preffs = df_preffs.convert_dtypes()
    df_preffs.index = list(map(lambda x: x.decode(), df_preffs.index))
    df_preffs["path"] = list(map(lambda x: x.decode(), df_preffs["path"]))
    df_preffs["path"] = df_preffs["path"].replace("", None)
    df_preffs["raw"] = df_preffs["raw"].replace(b"", None)

    df_preffs["size"] = df_preffs["size"].replace(-1, np.nan)
    df_preffs["offset"] = df_preffs["offset"].replace(-1, np.nan)
    df_preffs = df_preffs.reindex(columns=["path", "offset", "size", "raw"])

    if parquet_fn:
        df_preffs.to_parquet(parquet_fn)
    return df_preffs
