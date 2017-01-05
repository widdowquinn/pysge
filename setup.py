#!python
#
# setup.py
#
# Distutils setup script for the pysge module
#
# (c) L.Pritchard 2012-2017

from distutils.core import setup
setup(name="pysge",
      version="1.0.1",
      py_modules=["pysge"],
      description="Module for job scheduling with SGE",
      author="Leighton Pritchard",
      author_email="leighton.pritchard@hutton.ac.uk",
      url="http://www.hutton.ac.uk/",
      )
