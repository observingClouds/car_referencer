"""Console script for car_referencer."""
import argparse
import glob
import logging
import os
import sys

import pandas as pd
from multiformats import CID

from . import index as idx
from . import reference as ref


def main():
    """Console script for car_referencer."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--car_files",
        help="Collection of car files. Argument can contain wildcards.",
        required=True,
    )
    parser.add_argument(
        "-p",
        "--parquet",
        help="Parquet reference filename to be created.",
        required=True,
    )
    parser.add_argument(
        "-i",
        "--index",
        help="Index file to be created. Helps for temporary steps.",
        default=None,
    )
    parser.add_argument(
        "-r",
        "--cid",
        help=(
            "Root CID. Recursive references of this CID will be added to the reference"
            " filesystem"
        ),
        required=True,
    )
    args = parser.parse_args()
    if os.path.exists(args.index):
        logging.info(f"Index file {args.index} exists and will be used.")
        index_df = pd.read_parquet(args.index)
    else:
        logging.info("Creating index of available CIDs in given car files")
        index_df = idx.generate_index(glob.glob(args.car_files), args.index)

    logging.info("Create reference filesystem for {args['cid']}.")

    _ = ref.create_preffs(CID.decode(args.cid), index_df, parquet_fn=args.parquet)

    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
