# Changelog

## Unreleased

### New features

- Support CARv2 files. If a CARv2 file contains an index (`IndexSorted` or
  `MultihashIndexSorted`), it is used to locate the blocks; otherwise the
  data payload is scanned. In both cases only the section headers are read,
  which makes indexing CARv2 files faster than CARv1 files. CARv1 and CARv2
  files can be mixed
  ([#8](https://github.com/observingClouds/car_referencer/issues/8)).

### Bug fixes

- Fix `generate_index` with pandas 2.0 and newer, which removed
  `Index.is_monotonic`
  ([#27](https://github.com/observingClouds/car_referencer/pull/27)).

### Dependencies

- Require `dag-cbor<0.3`, `pure-protobuf<3` and `typing-validation<2`.
  Newer releases removed APIs that `ipldstore@unixfs` imports
  ([#27](https://github.com/observingClouds/car_referencer/pull/27)).

### Internal changes

- Fix the CI test environment: install from conda-forge only, and pin
  Python 3.13 and `zarr<3`
  ([#27](https://github.com/observingClouds/car_referencer/pull/27)).
- Update the pre-commit hooks and drop the yanked `types-pkg_resources`
  stub from the mypy hook
  ([#27](https://github.com/observingClouds/car_referencer/pull/27)).
