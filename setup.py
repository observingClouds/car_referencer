#!/usr/bin/env python

"""The setup script."""

import os

from setuptools import find_packages, setup
from setuptools.command.develop import develop

with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("requirements.txt") as f:
    requirements = f.read().strip().split("\n")

test_requirements = [
    "pytest>=3",
]


class PostDevelopCommand(develop):
    """Post-installation for development mode."""

    def run(self):
        develop.run(self)
        print("post dev command")
        os.system("go install -v github.com/Jorropo/linux2ipfs@latest")


setup(
    author="Hauke Schulz",
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    cmdclass={
        "develop": PostDevelopCommand,
    },
    description="Creating parquet file reference system for car collections.",
    entry_points={
        "console_scripts": [
            "car_referencer=car_referencer.cli:main",
        ],
    },
    install_requires=requirements,
    license="GNU General Public License v3",
    long_description=readme,
    include_package_data=True,
    keywords="car_referencer",
    name="car_referencer",
    packages=find_packages(include=["car_referencer", "car_referencer.*"]),
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/observingClouds/car_referencer",
    version="0.0.1",
    zip_safe=False,
)
