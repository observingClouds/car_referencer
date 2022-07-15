#!/usr/bin/env python

"""Tests for `car_referencer` package."""

import subprocess

import pytest
import xarray as xr

from car_referencer import car_referencer


def create_test_zarr():
    xr.tutorial.load_dataset("air_temperature").chunk(
        {"time": 100, "lat": 5, "lon": 5}
    ).to_zarr("example.zarr")


def create_test_cars(zarr_fn):
    subprocess.call("echo {} > old-example.json")
    subprocess.call(
        "~/go/bin/linux2ipfs -car-size 3382286 -driver car-example.%d.car -incremental-file old-example.json example.zarr"
    )


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
