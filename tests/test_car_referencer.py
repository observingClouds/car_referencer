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
