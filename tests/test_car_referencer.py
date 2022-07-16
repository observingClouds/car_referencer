#!/usr/bin/env python

"""Tests for `car_referencer` package."""

import glob
import os
import subprocess

import pytest
import xarray as xr

from car_referencer import car_referencer


def create_test_zarr(tmp_path):
    xr.tutorial.load_dataset("air_temperature").chunk(
        {"time": 100, "lat": 5, "lon": 5}
    ).to_zarr(str(tmp_path) + "/example.zarr")


@pytest.fixture
def test_cars(tmp_path):
    create_test_zarr(tmp_path)
    with open(f"{str(tmp_path)}/old-example.json", "w") as f:
        f.write("{}")
    if os.environ.get("GOPATH"):
        gopath = os.environ["GOPATH"] + "/go/bin"
    else:
        gopath = os.environ["HOME"] + "/go/bin"
    assert os.path.exists(gopath + "/linux2ipfs"), "linux2ipfs could not be found"
    subprocess.check_output(
        f"{gopath}/linux2ipfs -car-size 3382286 -driver car-{str(tmp_path)}/example.%d.car -incremental-file {str(tmp_path)}/old-example.json {str(tmp_path)}/example.zarr",
        shell=True,
    )
    return sorted(glob.glob(f"{str(tmp_path)}/example.*.car"))


@pytest.fixture
def response():
    """Sample pytest fixture.

    See more at: http://doc.pytest.org/en/latest/fixture.html
    """
    # import requests
    # return requests.get('https://github.com/audreyr/cookiecutter-pypackage')


def test_content(response):
    """Sample pytest test function with the pytest fixture as an argument."""
    # from bs4 import BeautifulSoup
    # assert 'GitHub' in BeautifulSoup(response.content).title.string


def test_index_creation(test_cars):
    print(test_cars)


def test_preffs_creation():
    pass
