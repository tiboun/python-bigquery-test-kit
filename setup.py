from glob import glob
from os import path
from os.path import basename, splitext

from setuptools import find_packages, setup

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()
    requirements_file = path.join(this_directory, 'requirements.txt')
    with open(requirements_file, encoding='utf-8') as r:
        install_requires = r.read()
        setup(
            name='bq-test-kit',
            version='0.2.0',
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
