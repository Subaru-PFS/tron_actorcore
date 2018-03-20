#!/usr/bin/env python3

"""Install this package. Requires sdss3tools.

To use:
python setup.py install
"""
import sdss3tools

sdss3tools.setup(
    description = "Common code base for PFS MHS actor system",
    name = "tron_actorcore",
    data_dirs = ['templateActor'],
    debug=True,
    install_lib="$base/python",
)
