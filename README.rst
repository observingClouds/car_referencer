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

Two formats of reference files are supported, selected with ``-f/--format``. By default the format is chosen by the extension of the output file: ``.json`` creates kerchunk references, everything else creates preffs references.

preffs
~~~~~~

.. code-block:: bash

    car_referencer -c "carfiles.*.car" -o preffs.parquet -r ROOT-HASH -i index.parquet

The created file ``preffs.parquet`` can be opened with `preffs <https://github.com/d70-t/preffs>`_, which needs to be installed separately (``pip install git+https://github.com/d70-t/preffs``):

.. code-block:: python

    import xarray as xr

    ds = xr.open_zarr("preffs::preffs.parquet")

kerchunk / fsspec
~~~~~~~~~~~~~~~~~

.. code-block:: bash

    car_referencer -c "carfiles.*.car" -o references.json -r ROOT-HASH -i index.parquet

The created file follows the `kerchunk reference specification (version 1) <https://fsspec.github.io/kerchunk/spec.html>`_ and can be opened with ``fsspec`` alone:

.. code-block:: python

    import xarray as xr

    ds = xr.open_dataset(
        "reference://",
        engine="zarr",
        backend_kwargs={
            "consolidated": False,
            "storage_options": {"fo": "references.json"},
        },
    )

In this format every file maps to a single byte range. Files that are split into several chunks inside the car files (e.g. large zarr chunks) can therefore not be referenced directly. ``car_referencer`` raises an error for such files unless ``--inline-chunked`` is given, which copies their content into the reference file. preffs references do not have this limitation.

Python usage
~~~~~~~~~~~~

.. code-block:: python

    from multiformats import CID

    from car_referencer.index import generate_index
    from car_referencer.reference import create_preffs, create_references

    index = generate_index(["carfiles.0.car", "carfiles.1.car"])
    create_preffs(CID.decode("ROOT-HASH"), index, parquet_fn="preffs.parquet")
    create_references(CID.decode("ROOT-HASH"), index, json_fn="references.json")


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
