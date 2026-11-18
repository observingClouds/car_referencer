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

from .car_utils import (
    assert_matches_ipfsspec,
    directory_node,
    file_node,
    raw_block,
    write_carv1,
)


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
    linux2ipfs = shutil.which("linux2ipfs")
    if linux2ipfs is None:
        gopath = subprocess.check_output(["go", "env", "GOPATH"], text=True).strip()
        linux2ipfs = os.path.join(gopath, "bin", "linux2ipfs")
    assert os.path.exists(linux2ipfs), "linux2ipfs could not be found"
    subprocess.check_output(
        f"{linux2ipfs} -car-size 3382286 -driver"
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


def test_zarr_references(zarr_cids, test_linux2ipfs_cars, tmp_test_folder):
    index = idx.generate_index(test_linux2ipfs_cars)
    zarr_root_cid = list(zarr_cids["cids"].values())[0]["cid"]
    json_fn = str(tmp_test_folder) + "/references.json"
    refs = ref.create_references(CID.decode(zarr_root_cid), index, json_fn=json_fn)
    assert refs["version"] == 1
    with open(json_fn) as f:
        assert json.load(f) == refs

    ds_refs = xr.open_dataset(
        "reference://",
        engine="zarr",
        backend_kwargs={"consolidated": False, "storage_options": {"fo": json_fn}},
    )
    ds_fs = xr.open_zarr(f"{str(tmp_test_folder)}/example.zarr")
    assert ds_fs.attrs == ds_refs.attrs
    assert_equal(ds_refs, ds_fs)


def test_codecs_references(
    cid="QmfZwnbqm2fmfBLtcfWT7fdr3VaPUB4fMB6SsypkiMNRFV",
    test_car=["tests/test.car"],
):
    index = idx.generate_index(test_car)
    cid = CID.decode(cid)
    # test.car contains a file that is split into several chunks
    with pytest.raises(ValueError, match="'multi' consists of"):
        ref.create_references(cid, index)
    refs = ref.create_references(cid, index, inline_chunked=True)
    fs = fsspec.filesystem("reference", fo=refs)
    files = fs.ls("", detail=False)
    assert files
    for file in files:
        assert fs.cat(file) == b"ipfsspec test data"


def test_multi_entry_references(multi_entry_cars):
    files, root, expected = multi_entry_cars
    index = idx.generate_index(files)

    # kerchunk references cannot point to several byte ranges per file
    with pytest.raises(ValueError, match="'chunked' consists of 4 chunks"):
        ref.create_references(root, index)

    refs = ref.create_references(root, index, inline_chunked=True)
    path, offset, size = refs["refs"]["single"]
    assert path in files
    assert size == len(expected["single"])
    assert isinstance(refs["refs"]["chunked"], str)  # inlined

    fs = fsspec.filesystem("reference", fo=refs)
    for key, content in expected.items():
        assert fs.cat(key) == content


def test_inline_encoding():
    assert ref._inline(b"text") == "text"
    assert ref._inline(b"\x00\xff") == "base64:AP8="
    # text that looks like base64 encoded data must be encoded itself
    assert ref._inline(b"base64:abc") == "base64:YmFzZTY0OmFiYw=="


@pytest.mark.parametrize(
    "output, extra_args, reader",
    [
        ("refs.parquet", [], "preffs"),
        ("refs.json", [], "reference"),
        ("refs.kerchunk", ["-f", "kerchunk", "--inline-chunked"], "reference"),
    ],
)
def test_cli(multi_entry_cars, tmp_path, monkeypatch, output, extra_args, reader):
    files, root, expected = multi_entry_cars
    output = str(tmp_path / output)
    if reader == "reference" and "--inline-chunked" not in extra_args:
        # the chunked file cannot be referenced in kerchunk format
        with pytest.raises(ValueError):
            _run_cli(monkeypatch, files, root, output, extra_args)
        extra_args = extra_args + ["--inline-chunked"]
    assert _run_cli(monkeypatch, files, root, output, extra_args) == 0

    if reader == "preffs":
        m = fsspec.get_mapper(f"preffs::{output}")
    else:
        m = fsspec.get_mapper("reference://", fo=output)
    for key, content in expected.items():
        assert m[key] == content


def _run_cli(monkeypatch, files, root, output, extra_args):
    from car_referencer import cli

    car_glob = files[0].replace(".0.car", ".*.car")
    argv = ["car_referencer", "-c", car_glob, "-o", output, "-r", str(root)]
    monkeypatch.setattr("sys.argv", argv + extra_args)
    return cli.main()


def test_linux2ipfs_cars_match_ipfsspec(test_linux2ipfs_cars):
    assert sum(map(assert_matches_ipfsspec, test_linux2ipfs_cars)) > 0
