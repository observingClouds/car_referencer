#!/usr/bin/env python

"""Tests for `car_referencer` package."""

import glob
import json
import os
import shutil
import subprocess

import pandas as pd
import pytest
import xarray as xr
from multiformats import CID
from xarray.testing import assert_allclose, assert_equal

import car_referencer.index as idx
import car_referencer.reference as ref


@pytest.fixture(scope="session")
def tmp_test_folder(tmpdir_factory):
    fn = tmpdir_factory.mktemp("test")
    return fn
    shutil.rmtree(str(fn))


def create_test_zarr(tmp_test_folder):
    xr.tutorial.load_dataset("air_temperature").chunk(
        {"time": 100, "lat": 5, "lon": 5}
    ).to_zarr(str(tmp_test_folder) + "/example.zarr")


@pytest.fixture(scope="session")
def test_cars(tmp_test_folder):
    create_test_zarr(tmp_test_folder)
    with open(f"{str(tmp_test_folder)}/old-example.json", "w") as f:
        f.write("{}")
    if os.environ.get("GOPATH"):
        gopath = os.environ["GOPATH"] + "/go/bin"
    else:
        gopath = os.environ["HOME"] + "/go/bin"
    assert os.path.exists(gopath + "/linux2ipfs"), "linux2ipfs could not be found"
    subprocess.check_output(
        f"{gopath}/linux2ipfs -car-size 3382286 -driver car-{str(tmp_test_folder)}/example.%d.car -incremental-file {str(tmp_test_folder)}/old-example.json {str(tmp_test_folder)}/example.zarr",
        shell=True,
    )
    return sorted(glob.glob(f"{str(tmp_test_folder)}/example.*.car"))


@pytest.fixture
def zarr_cids(tmp_test_folder):
    with open(f"{str(tmp_test_folder)}/old-example.json") as f:
        zarr_object_cids = json.load(f)
    return zarr_object_cids


def test_index_creation(test_cars):
    print(test_cars)
    index = idx.generate_index(test_cars)
    assert index is not None
    assert isinstance(index, pd.DataFrame)
    return index


def test_preffs_creation(zarr_cids, test_cars, tmp_test_folder):
    index = idx.generate_index(test_cars)
    assert isinstance(index, pd.DataFrame)
    zarr_root_cid = list(zarr_cids["cids"].values())[0]["cid"]
    preff_df = ref.create_preffs(
        CID.decode(zarr_root_cid),
        index,
        parquet_fn=str(tmp_test_folder) + "/preff.parquet",
    )
    print(preff_df)


def test_preffs(tmp_test_folder):
    ds_preffs = xr.open_zarr(f"preffs::{str(tmp_test_folder)}/preff.parquet")
    ds_fs = xr.open_zarr(f"{str(tmp_test_folder)}/example.zarr")
    assert ds_fs.attrs == ds_preffs.attrs
    assert_equal(ds_preffs, ds_fs)
