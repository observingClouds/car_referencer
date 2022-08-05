import base64

import pandas as pd
from ipldstore.unixfsv1 import Data, PBNode
from multiformats import CID

verbose = False


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


def loop_create_reffs(cid, index, ref_fs=[], dir=None):
    if cid.codec.name == "raw":
        if cid.hashfun.codec.name == "identity":
            if verbose:
                print(
                    dir,
                    base64.b64decode(base64.b64encode(cid.raw_digest).decode("ascii")),
                )
            ref_fs.append(
                [
                    dir,
                    None,
                    None,
                    None,
                    base64.b64decode(base64.b64encode(cid.raw_digest).decode("ascii")),
                ]
            )
        else:
            if verbose:
                print(dir, index.loc[cid.digest])
            entry = index.loc[cid.digest]
            if isinstance(entry, pd.DataFrame):
                entry = entry.sort_values("order")
                ref_fs.extend(
                    [
                        [
                            dir,
                            e.file,
                            e.offset,
                            e["size"],
                            None,
                        ]
                        for _, e in entry.iterrows()
                    ]
                )
            else:  # single entry
                ref_fs.extend(
                    [
                        [
                            dir,
                            entry.file,
                            entry.offset,
                            entry["size"],
                            None,
                        ]
                    ]
                )
    else:
        node, data = create_reference_fs(cid, index)
        len_links = len(list_links(node))
        if cid.codec.name == "dag-pb" and len_links > 0:
            for n, (name, hash) in enumerate(list_links(node)):
                ref_fs = loop_create_reffs(
                    hash, index, ref_fs, dir="/".join(filter(bool, [dir, name]))
                )
        elif cid.codec.name == "dag-pb" and len_links == 0:
            if verbose:
                print(dir, data.Data)
            # ref_fs.extend(
            #                    [
            #                        [
            #                            dir,
            #                            None,
            #                            None,
            #                            None,
            #                            data.Data,
            #                        ]
            #                    ]
            #                ) # or use entry = index.loc[cid.digest] and cut of header in entry
            entry = index.loc[cid.digest]
            ref_fs.extend(
                [
                    [
                        dir,
                        entry["file"],
                        entry["offset"] + 6,
                        data.filesize,
                        None,
                    ]
                ]
            )
        else:
            raise ValueError(f"cid codec {cid.codec.name} not known")
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
