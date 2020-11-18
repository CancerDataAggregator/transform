import pathlib
from datetime import datetime
from setuptools import setup, find_packages

current_path = pathlib.Path(__file__).parent

name = "cdatransform"
version = open("cdatransform/version.py").read().split("=")[1].strip().strip("\"")
now = datetime.utcnow()
desc_path = pathlib.Path(current_path, "README.md")
long_description = desc_path.open("r").read()

setup(
    name=name,
    version=version,
    packages=find_packages(),
    platforms=['POSIX', 'MacOS', 'Windows'],
    python_requires='>=3.6',
    install_requires=[
    ],
    entry_points={
        'console_scripts': [
            'cda-transform = cdatransform.main:main'
        ],
    },

    description='Performs transforms to hamonize DC data to CDA',
    long_description=long_description,
    long_description_content_type="text/markdown",
    include_package_data=True,
)