from itertools import chain

import ipldstore
import numpy as np
import pandas as pd
import tqdm
from ipldstore.unixfsv1 import Data, PBLink, PBNode
from multiformats import CID


def read_car(carfile):
    with open(carfile, "rb") as f:
        roots, blocks = ipldstore.car.read_car(f)
        for cid, data, location in blocks:
            yield cid.digest, carfile, location.payload_offset, location.payload_size


def save_index(df, filename):
    df.to_parquet(filename)


def generate_index(carfiles, index_fn=None):
    df = pd.DataFrame(
        tqdm.tqdm(chain.from_iterable(map(read_car, carfiles))),
        columns=["cid", "file", "offset", "size"],
    )
    df["order"] = np.arange(len(df))  # needed for duplicate entries
    df = df.set_index("cid")
    if index_fn:
        save_index(df, index_fn)
    return df
