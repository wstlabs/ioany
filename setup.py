import os
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "ioany",
    version = "0.0.1",
    author = "wstlabs",
    author_email = "wst@pobox.com",
    description = ("Painless slurping and conversion between CSV, JSON and row/record formats"),
    license = "BSD",
    keywords = "dataframe csv json iterable stream-oriented",
    url = "http://packages.python.org/ioany",
    packages= ['ioany', 'ioany.util', 'tests'],
    long_description=read('README.rst'),
    classifiers= [
        "Development Status :: 2 - Pre-Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: BSD License",
    ],
)

