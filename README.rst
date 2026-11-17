==============
car_referencer: Creating parquet file reference system for car collections.
==============

.. image:: https://github.com/observingClouds/car_referencer/actions/workflows/ci.yaml/badge.svg
        :target: https://github.com/observingClouds/car_referencer/actions
        :alt: Github-CI Status


.. warning:
    Note this project is still under development and needs further testing.

Similar to tape archive (tar) files, `content addressable archive <https://ipld.io/specs/transport/car/>`_ (car) files are a possibility to group objects to larger quantities.
Besides uploading these car files to an object store, they also pose the possibility to save the collections of objects on a traditional filesystem. Accessing these collections without the need of extracting the individual objects can be realized by the usage of a reference file system.

``car_referencer`` can create the needed reference file from single ``car`` s or multiple ``car`` s that are part of the same merkle DAG.
Both `CARv1 <https://ipld.io/specs/transport/car/carv1/>`_ and `CARv2 <https://ipld.io/specs/transport/car/carv2/>`_ files are supported. For CARv2 files the included index is used to speed up the creation of the index file.

Command line usage
------------------

``car_referencer`` creates the reference file internally in two steps. The first step is to identify all available references within the provided ``car`` s ( here ``carfiles.*.car``) and save this as an index file (e.g. ``index.parquet``) that will be reused if it already exists. In a second step the reference file (e.g. ``preffs.parquet``) is created based on the ``ROOT-HASH`` that identifies NOT the root-CID of the car file, but the root-CID of the root file-object. In case of a zarr file, like ``example.zarr``, the ROOT-CID would refer to ``example.zarr`` itself.

.. code-block:: bash

    car_referencer -c "carfiles.*.car" -p preffs.parquet -r ROOT-HASH -i index.parquet

The created file ``preffs.parquet`` can then be opened by

.. code-block:: python

    import xarray as xr

    ds = xr.open_zarr("preffs::preffs.parquet")

thanks to https://github.com/d70-t/preffs.

fsspec filesystem for indexed CARv2 files
-----------------------------------------

CARv2 files that contain an index can also be opened directly, without
creating a reference file first. The ``car`` filesystem reads the CARv2 header
and the index and then serves the UnixFS directories and files of the DAG with
ranged reads into the CAR file(s). The CAR files can be stored on any fsspec
filesystem:

.. code-block:: python

    import fsspec
    import xarray as xr

    fs = fsspec.filesystem("car", fo="s3://bucket/example.car")
    fs.ls("/")

    # or use URL chaining
    ds = xr.open_zarr("car://example.zarr::s3://bucket/example.car")

By default, the first root listed in the CAR header is the root of the
filesystem. Use ``root="<CID>"`` to choose another UnixFS node, and pass a list
of files as ``fo`` if the blocks of the DAG are distributed over several CAR
files. An index can be added to existing CAR files with
``car index`` from `go-car <https://github.com/ipld/go-car>`_.


Installation
------------

.. code-block:: bash

    git clone https://github.com/observingClouds/car_referencer.git
    cd car_referencer
    pip install .

Development
-----------

The development environment is managed with `uv <https://docs.astral.sh/uv/>`_. The tests additionally need `linux2ipfs <https://github.com/Jorropo/linux2ipfs>`_, which is written in go:

.. code-block:: bash

    git clone https://github.com/observingClouds/car_referencer.git
    cd car_referencer
    uv sync
    go install github.com/Jorropo/linux2ipfs@latest
    uv run pytest

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
