import base64

import h5py
import numpy as np
import pandas as pd
import tables
from ipldstore.unixfsv1 import Data, PBNode
from multiformats import CID

verbose = True


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


class Reference(tables.IsDescription):
    key = tables.StringCol(1000)
    path = tables.StringCol(1000)
    offset = tables.Float64Col(dflt=-1)
    size = tables.Float64Col(dflt=-1)
    raw = tables.StringCol(10000)


def loop_create_reffs(cid, index, table, dir=None):
    reference = table.row
    if cid.codec.name == "raw":
        if cid.hashfun.codec.name == "identity":
            if verbose:
                print(
                    "here",
                    dir,
                    base64.b64decode(base64.b64encode(cid.raw_digest).decode("ascii")),
                )
                print(
                    "length",
                    len(
                        base64.b64decode(
                            base64.b64encode(cid.raw_digest).decode("ascii")
                        )
                    ),
                )
            reference["key"] = dir
            # reference["path"] = entry["file"]
            # reference["offset"] = entry["offset"]+6
            # reference["size"] = data.filesize
            reference["raw"] = base64.b64decode(
                base64.b64encode(cid.raw_digest).decode("ascii")
            )
            reference.append()
        else:
            if verbose:
                print(dir, index.loc[cid.digest])
            entry = index.loc[cid.digest]
            if isinstance(entry, pd.DataFrame):
                entry = entry.sort_values("order")
                for _, e in entry.iterrows():
                    reference["key"] = dir
                    reference["path"] = e.file
                    reference["offset"] = e.offset
                    reference["size"] = e["size"]
                    # reference["raw"] = None
                    reference.append()
            else:  # single entry per zarr-file
                reference["key"] = dir
                reference["path"] = entry.file
                reference["offset"] = entry.offset
                reference["size"] = entry["size"]
                # reference["raw"] = None
                reference.append()
    else:
        node, data = create_reference_fs(cid, index)
        len_links = len(list_links(node))
        if cid.codec.name == "dag-pb" and len_links > 0:
            for n, (name, hash) in enumerate(list_links(node)):
                table = loop_create_reffs(
                    hash, index, table, dir="/".join(filter(bool, [dir, name]))
                )
                if n > 10000:
                    table.flush()

        elif cid.codec.name == "dag-pb" and len_links == 0:
            if verbose:
                print(dir, data.Data)

            entry = index.loc[cid.digest]
            reference["key"] = dir
            reference["path"] = entry["file"]
            reference["offset"] = entry["offset"] + 6
            reference["size"] = data.filesize
            reference.append()
        else:
            raise ValueError(f"cid codec {cid.codec.name} not known")

    return table


def create_preffs(cid, index, parquet_fn=None):
    h5file = tables.open_file(
        "car_references.h5.partial", mode="w", title="Car references"
    )
    table = h5file.create_table("/", "references", Reference, "Readout example")

    table = loop_create_reffs(cid, index, table)
    table.flush()
    h5 = h5py.File("car_references.h5.partial")
    df_preffs = pd.DataFrame.from_records(
        h5["references"], columns=["key", "offset", "path", "raw", "size"], index="key"
    )
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
