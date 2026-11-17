#!/usr/bin/env python

"""Tests for `car_referencer` package."""

import glob
import json
import os
import shutil
import subprocess

import fsspec
import pandas as pd
import pytest
import xarray as xr
from multiformats import CID
from xarray.testing import assert_equal

import car_referencer.index as idx
import car_referencer.reference as ref

from .car_utils import directory_node, file_node, raw_block, write_carv1


@pytest.fixture(scope="session")
def tmp_test_folder(tmpdir_factory):
    fn = tmpdir_factory.mktemp("test")
    return fn
    shutil.rmtree(str(fn))


def create_test_zarr_simple(tmp_test_folder):
    xr.tutorial.load_dataset("air_temperature").chunk(
        {"time": 100, "lat": 5, "lon": 5}
    ).to_zarr(str(tmp_test_folder) + "/example.zarr")


def create_test_zarr_complex(tmp_test_folder):
    ds = xr.tutorial.load_dataset("air_temperature")
    encoding = {var: {"compressor": None} for var in ds.variables}
    ds.chunk().to_zarr(
        str(tmp_test_folder) + "/example.zarr", mode="w", encoding=encoding
    )


@pytest.fixture(scope="session")
def test_linux2ipfs_cars(tmp_test_folder, creator=create_test_zarr_simple):
    """
    Create car files with linux2ipfs

    Input
    -----
    tmp_test_folder : str
      temporary folder where car file should be written to
    creator : function
      function that creates a specific zarr file
    """

    creator(tmp_test_folder)
    with open(f"{str(tmp_test_folder)}/old-example.json", "w") as f:
        f.write("{}")
    if os.environ.get("GOPATH"):
        gopath = os.environ["GOPATH"] + "/go/bin"
    else:
        gopath = os.environ["HOME"] + "/go/bin"
    assert os.path.exists(gopath + "/linux2ipfs"), "linux2ipfs could not be found"
    subprocess.check_output(
        f"{gopath}/linux2ipfs -car-size 3382286 -driver"
        f" car-{str(tmp_test_folder)}/example.%d.car -incremental-file"
        f" {str(tmp_test_folder)}/old-example.json {str(tmp_test_folder)}/example.zarr",
        shell=True,
    )
    return sorted(glob.glob(f"{str(tmp_test_folder)}/example.*.car"))


@pytest.fixture
def zarr_cids(tmp_test_folder):
    with open(f"{str(tmp_test_folder)}/old-example.json") as f:
        zarr_object_cids = json.load(f)
    return zarr_object_cids


def test_index_creation(test_linux2ipfs_cars):
    print(test_linux2ipfs_cars)
    index = idx.generate_index(test_linux2ipfs_cars)
    assert index is not None
    assert isinstance(index, pd.DataFrame)
    return index


def test_zarr_preffs_creation(zarr_cids, test_linux2ipfs_cars, tmp_test_folder):
    index = idx.generate_index(test_linux2ipfs_cars)
    assert isinstance(index, pd.DataFrame)
    zarr_root_cid = list(zarr_cids["cids"].values())[0]["cid"]
    preff_df = ref.create_preffs(
        CID.decode(zarr_root_cid),
        index,
        parquet_fn=str(tmp_test_folder) + "/preff.parquet",
    )
    print(preff_df)


def test_zarr_preffs(tmp_test_folder):
    ds_preffs = xr.open_zarr(f"preffs::{str(tmp_test_folder)}/preff.parquet")
    ds_fs = xr.open_zarr(f"{str(tmp_test_folder)}/example.zarr")
    assert ds_fs.attrs == ds_preffs.attrs
    assert_equal(ds_preffs, ds_fs)


def test_codecs_preffs_creation(
    tmp_test_folder,
    cid="QmfZwnbqm2fmfBLtcfWT7fdr3VaPUB4fMB6SsypkiMNRFV",
    test_car=["tests/test.car"],
):
    index = idx.generate_index(test_car)
    assert isinstance(index, pd.DataFrame)
    cid = CID.decode(cid)
    preff_df = ref.create_preffs(
        cid,
        index,
        parquet_fn=str(tmp_test_folder) + "/testcar_preff.parquet",
    )
    print(preff_df)


def test_codecs_preffs(tmp_test_folder):
    m = fsspec.get_mapper(f"preffs::{str(tmp_test_folder)}/testcar_preff.parquet")
    files = m.fs.ls("/")
    for file in files:
        print(file["name"], m.fs.cat(file["name"]))
        assert m.fs.cat(file["name"]) == b"ipfsspec test data"


@pytest.fixture(scope="session")
def multi_entry_cars(tmp_test_folder):
    """Car files with a file node that links to multiple chunks.

    The file ``chunked`` consists of the chunks ``a, b, a, c`` i.e. the file
    node has multiple entries, one of which is repeated. The DAG is split into
    two car files and chunk ``b`` is contained in both of them, such that the
    index has multiple entries for the same CID.
    """
    a = raw_block(b"first chunk|")
    b = raw_block(b"second chunk|")
    c = raw_block(b"third and last chunk")
    chunked = file_node([a, b, a, c])
    single = file_node([b])
    root = directory_node({"chunked": chunked, "single": single})

    files = [
        str(tmp_test_folder) + "/multi_entry.0.car",
        str(tmp_test_folder) + "/multi_entry.1.car",
    ]
    write_carv1(files[0], [root[0]], [root, chunked, a, b])
    write_carv1(files[1], [root[0]], [single, b, c])
    expected = {
        "chunked": a[1] + b[1] + a[1] + c[1],
        "single": b[1],
    }
    return files, root[0], expected


def test_multi_entry_index(multi_entry_cars):
    files, _, _ = multi_entry_cars
    index = idx.generate_index(files)
    assert index.index.is_unique
    assert index.index.is_monotonic_increasing
    assert len(index) == 6


def test_multi_entry_preffs(multi_entry_cars, tmp_test_folder):
    files, root, expected = multi_entry_cars
    index = idx.generate_index(files)
    parquet_fn = str(tmp_test_folder) + "/multi_entry_preff.parquet"
    preff_df = ref.create_preffs(root, index, parquet_fn=parquet_fn)

    # one reference per chunk, in the order of the links of the file node
    assert list(preff_df.loc["chunked", "size"]) == [12, 13, 12, 20]
    assert preff_df.loc["single", "size"] == 13

    m = fsspec.get_mapper(f"preffs::{parquet_fn}")
    assert sorted(m.keys()) == sorted(expected.keys())
    for key, content in expected.items():
        assert m[key] == content
