# Copyright (c) 2020 Bounkong Khamphousone
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from glob import glob
from io import TextIOWrapper
from os import path
from os.path import basename, splitext

import yaml
from setuptools import find_packages, setup

this_directory = path.abspath(path.dirname(__file__))


def read(filename: str) -> TextIOWrapper:
    """
        Open the filename relative to this directory.
    Args:
        filename (str): filename to open

    Returns:
        TextIOWrapper: file reader
    """
    return open(path.join(this_directory, filename), encoding='utf-8')


with read('README.md') as f:
    long_description = f.read()
    with read('requirements.txt') as r:
        install_requires = r.read()
        with read('.cz.yaml') as cz:
            cz_content = yaml.load(cz, Loader=yaml.SafeLoader)
            setup(
                name='bq-test-kit',
                version=cz_content['commitizen']['version'],
                url='https://github.com/tiboun/python-bq-test-kit',
                author='Bounkong Khamphousone',
                author_email='bounkong@gmail.com',
                py_modules=[splitext(basename(p))[0] for p in glob('src/*.py')],
                packages=find_packages('src'),
                package_dir={'': 'src'},
                include_package_data=True,
                zip_safe=False,
                description=("BigQuery test kit"),
                license="MIT",
                long_description=long_description,
                install_requires=install_requires,
                long_description_content_type='text/markdown',
                extras_require={
                    'shell':  ["varsubst"],
                    'jinja2':  ["varsubst[jinja2]"]
                },
                classifiers=[
                    'Development Status :: 4 - Beta',
                    'Intended Audience :: Developers',
                    'Topic :: Software Development :: Testing',
                    'License :: OSI Approved :: MIT License',
                    'Programming Language :: Python :: 3'
                ],
                keywords='BigQuery testing test-kit bqtk dataset table isolation dsl immutability',
                python_requires='>=3'
            )
