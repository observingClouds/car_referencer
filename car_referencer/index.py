from itertools import chain

import ipldstore
import numpy as np
import pandas as pd
import tqdm


def read_car(carfile):
    with open(carfile, "rb") as f:
        roots, blocks = ipldstore.car.read_car(f)
        for cid, data, location in blocks:
            yield cid.digest, carfile, location.payload_offset, location.payload_size


def save_index(df, filename):
    df.to_parquet(filename)


def generate_index(carfiles, index_fn=None):
    index = pd.DataFrame(
        tqdm.tqdm(chain.from_iterable(map(read_car, carfiles))),
        columns=["cid", "file", "offset", "size"],
    )
    index["order"] = np.arange(len(index))
    index = index.set_index("cid")
    index = index.sort_index(kind="stable")
    # The same block can be contained in several car files. All copies have
    # the same content, so only the first occurrence is kept.
    index = index[~index.index.duplicated(keep="first")]
    assert index.index.is_monotonic_increasing
    if index_fn:
        save_index(index, index_fn)
    return index
