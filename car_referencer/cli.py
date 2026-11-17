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
        "-o",
        "--output",
        "-p",
        "--parquet",
        dest="output",
        help="Reference filename to be created.",
        required=True,
    )
    parser.add_argument(
        "-f",
        "--format",
        choices=["preffs", "kerchunk"],
        default=None,
        help=(
            "Format of the reference file: 'preffs' (parquet) or 'kerchunk'"
            " (fsspec reference JSON). Default: 'kerchunk' if the output"
            " filename ends with '.json', otherwise 'preffs'."
        ),
    )
    parser.add_argument(
        "--inline-chunked",
        action="store_true",
        help=(
            "kerchunk format only: copy the content of files that consist of"
            " several chunks into the reference file. Without this option such"
            " files raise an error, as kerchunk references can only point to a"
            " single byte range per file."
        ),
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
    fmt = args.format
    if fmt is None:
        fmt = "kerchunk" if args.output.endswith(".json") else "preffs"
    if args.inline_chunked and fmt != "kerchunk":
        parser.error("--inline-chunked is only supported for the kerchunk format")

    if args.index and os.path.exists(args.index):
        logging.info(f"Index file {args.index} exists and will be used.")
        index_df = pd.read_parquet(args.index)
    else:
        logging.info("Creating index of available CIDs in given car files")
        index_df = idx.generate_index(glob.glob(args.car_files), args.index)

    logging.info(f"Create {fmt} reference filesystem for {args.cid}.")

    root = CID.decode(args.cid)
    if fmt == "kerchunk":
        ref.create_references(
            root, index_df, json_fn=args.output, inline_chunked=args.inline_chunked
        )
    else:
        ref.create_preffs(root, index_df, parquet_fn=args.output)

    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
