==============
car_referencer
==============


.. image:: https://img.shields.io/pypi/v/car_referencer.svg
        :target: https://pypi.python.org/pypi/car_referencer

.. image:: https://img.shields.io/travis/observingClouds/car_referencer.svg
        :target: https://travis-ci.com/observingClouds/car_referencer

.. image:: https://readthedocs.org/projects/car-referencer/badge/?version=latest
        :target: https://car-referencer.readthedocs.io/en/latest/?version=latest
        :alt: Documentation Status




Creating parquet file reference system for car collections.


Note this project is still under heavy development and not yet tested properly.

Installation
------------

```
git clone https://github.com/observingClouds/car_referencer.git
cd car_referencer
pip install .
```

Development
-----------

For testing purposes additional dependencies need to be installed including some packages written in go. The needed environment can be installed by

```
git clone https://github.com/observingClouds/car_referencer.git
cd car_referencer
mamba env create
source activate test-env
```

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
