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

Command line usage
------------------

``car_referencer`` creates the reference file internally in two steps. The first step is to identify all available references within the provided ``car`` s ( here ``carfiles.*.car``) and save this as an index file (e.g. ``index.parquet``) that will be reused if it already exists. In a second step the reference file (e.g. ``preffs.parquet``) is created based on the ``ROOT-HASH`` that identifies NOT the root-CID of the car file, but the root-CID of the root file-object. In case of a zarr file, like ``example.zarr``, the ROOT-CID would refer to ``example.zarr`` itself.

.. code-block:: bash

    car_referencer -c "carfiles.*.car" -p preffs.parquet -r ROOT-HASH -i index.parquet

The created file ``preffs.parquet`` can then be opened by

.. code-block:: python

    import xarray as xr

    ds = xr.open_zarr("preffs:preffs.parquet")

thanks to https://github.com/d70-t/preffs.


Installation
------------

.. code-block:: bash

    git clone https://github.com/observingClouds/car_referencer.git
    cd car_referencer
    pip install .

Development
-----------

For testing purposes additional dependencies need to be installed including some packages written in go. The needed environment can be installed by

.. code-block:: bash

    git clone https://github.com/observingClouds/car_referencer.git
    cd car_referencer
    mamba env create
    source activate test-env

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
