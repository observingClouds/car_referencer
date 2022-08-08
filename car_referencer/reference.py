import base64

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
    key = tables.StringCol(100)
    path = tables.StringCol(100)
    offset = tables.Int64Col()
    size = tables.Int64Col()
    raw = tables.StringCol(100)


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
                    reference["raw"] = None
                    table.append()
            else:  # single entry per zarr-file
                reference["key"] = "/".join(filter(bool, [dir, name]))
                reference["path"] = entry.file
                reference["offset"] = entry.offset
                reference["size"] = entry["size"]
                reference["raw"] = None
                table.append()
        elif hash.hashfun.code == 0:  # identity
            reference["key"] = "/".join(filter(bool, [dir, name]))
            reference["path"] = None
            reference["offset"] = None
            reference["size"] = None
            reference["raw"] = base64.b64decode(
                base64.b64encode(hash.raw_digest).decode("ascii")
            )
            table.append()
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

    # df_preffs = pd.DataFrame.from_records(
    #    ref_fs, columns=["key", "path", "offset", "size", "raw"], index="key"
    # )
    # df_preffs = df_preffs.sort_index(kind="stable")
    # df_preffs = df_preffs.convert_dtypes()
    # if parquet_fn:
    #    df_preffs.to_parquet(parquet_fn)
    # return df_preffs
