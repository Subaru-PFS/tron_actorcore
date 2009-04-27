#!/usr/bin/env python
"""Install this package. Requires sdss3tools.

To use:
python setup.py install
"""
import sdss3tools

sdss3tools.setup(
    description = "Common code base for SDSS-III actor system",
)
