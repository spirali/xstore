#!/usr/bin/env python

from setuptools import setup
import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def read(fname):
    return open(os.path.join(ROOT_DIR, fname)).read()


with open('requirements.txt') as reqs:
    requirements = [line.strip().split("==")[0] for line in reqs.readlines()]


setup(name='orco',
      version="0.0.1",
      description='Organized computing',
      author='Stanislav Bohm',
      url='http://github.com/spirali/orco',
      packages=['orco'],
      install_requires=requirements,
      classifiers=("Programming Language :: Python :: 3",
                   "License :: OSI Approved :: MIT License",
                   "Operating System :: OS Independent"))
