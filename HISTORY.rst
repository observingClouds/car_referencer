=======
History
=======

Unreleased
----------

Bug fixes
~~~~~~~~~

* Fix ``generate_index`` with pandas 2.0 and newer, which removed
  ``Index.is_monotonic``
  (`#27 <https://github.com/observingClouds/car_referencer/pull/27>`__).

Dependencies
~~~~~~~~~~~~

* Require ``dag-cbor<0.3``, ``pure-protobuf<3`` and ``typing-validation<2``.
  Newer releases removed APIs that ``ipldstore@unixfs`` imports
  (`#27 <https://github.com/observingClouds/car_referencer/pull/27>`__).

Internal changes
~~~~~~~~~~~~~~~~

* Fix the CI test environment: install from conda-forge only, and pin
  Python 3.13 and ``zarr<3``
  (`#27 <https://github.com/observingClouds/car_referencer/pull/27>`__).
* Update the pre-commit hooks and drop the yanked ``types-pkg_resources``
  stub from the mypy hook
  (`#27 <https://github.com/observingClouds/car_referencer/pull/27>`__).
