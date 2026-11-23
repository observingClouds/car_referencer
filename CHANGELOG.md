# Changelog

## Unreleased

### New features

- `generate_index(..., verify=True)` and `car_referencer --verify` check all
  blocks against their CIDs while creating the index.
- Support CARv2 files. If a CARv2 file contains an index (`IndexSorted` or
  `MultihashIndexSorted`), it is used to locate the blocks; otherwise the
  data payload is scanned. CARv1 and CARv2 files can be mixed
  ([#8](https://github.com/observingClouds/car_referencer/issues/8)).

### Bug fixes

- Fix reference creation when the same block is contained in several car
  files. `generate_index` now keeps only the first occurrence of each CID
  ([#9](https://github.com/observingClouds/car_referencer/issues/9)).
- Fix `generate_index` with pandas 2.0 and newer, which removed
  `Index.is_monotonic`
  ([#27](https://github.com/observingClouds/car_referencer/pull/27)).

### Dependencies

- Drop support for Python 3.10; Python 3.11 is now the minimum. Python 3.14
  is tested in CI.
- `ipldstore` is no longer a dependency. car_referencer reads CARv1 files
  and decodes dag-pb/UnixFS nodes itself. This also removes the pins of
  `dag-cbor`, `pure-protobuf` and `typing-validation`. CARv1 files are now
  indexed by reading only the section headers, like CARv2 files; blocks are
  no longer hashed unless `verify=True` is given. The tests use `ipfsspec`
  from PyPI instead of `ipldstore` to build and cross-check car files.

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
