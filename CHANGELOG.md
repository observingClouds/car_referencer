# Changelog

## Unreleased

### New features

- Support CARv2 files. If a CARv2 file contains an index (`IndexSorted` or
  `MultihashIndexSorted`), it is used to locate the blocks; otherwise the
  data payload is scanned. In both cases only the section headers are read,
  which makes indexing CARv2 files faster than CARv1 files. CARv1 and CARv2
  files can be mixed
  ([#8](https://github.com/observingClouds/car_referencer/issues/8)).
- Add `CARFileSystem`, an fsspec filesystem (protocol `car`) for indexed
  CARv2 files. It reads only the CARv2 header and index up front and serves
  UnixFS directories and files with ranged reads directly from the CAR
  file(s), which can be stored on any fsspec filesystem. No separate
  reference file is needed, e.g.
  `xr.open_zarr("car://example.zarr::s3://bucket/example.car")`.

### Bug fixes

- Fix reference creation when the same block is contained in several car
  files. `generate_index` now keeps only the first occurrence of each CID
  ([#9](https://github.com/observingClouds/car_referencer/issues/9)).
- Fix `generate_index` with pandas 2.0 and newer, which removed
  `Index.is_monotonic`
  ([#27](https://github.com/observingClouds/car_referencer/pull/27)).

### Dependencies

- Require `dag-cbor<0.3`, `pure-protobuf<3` and `typing-validation<2`.
  Newer releases removed APIs that `ipldstore@unixfs` imports
  ([#27](https://github.com/observingClouds/car_referencer/pull/27)).

### Internal changes

- Add tests for file nodes with multiple (and repeated) chunks that are
  spread across several car files
  ([#9](https://github.com/observingClouds/car_referencer/issues/9)).
- Move from `setup.py`, `requirements.txt` and a conda environment to
  `pyproject.toml` and [uv](https://docs.astral.sh/uv/) with a `uv.lock`.
  CI installs go with `actions/setup-go` instead of conda, as
  `setup-miniconda` no longer works on the GitHub runners. Pin
  `xarray<2026.9` for the tests, as newer releases require `zarr>=3`.
- Fix the CI test environment: install from conda-forge only, and pin
  Python 3.13 and `zarr<3`
  ([#27](https://github.com/observingClouds/car_referencer/pull/27)).
- Update the pre-commit hooks and drop the yanked `types-pkg_resources`
  stub from the mypy hook
  ([#27](https://github.com/observingClouds/car_referencer/pull/27)).
