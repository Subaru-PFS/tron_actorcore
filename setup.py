#!/usr/bin/env python
"""Install this package. Requires sdss3tools.

To use:
python setup.py install
"""
import sdss3tools

sdss3tools.setup(
    description = "Common code base for PFS MHS actor system",
    name = "ics_mhs_actorcore",
    packages = ['ics_mhs_actorcore',
                'ics_mhs_actorcore.RO',
                'ics_mhs_actorcore.actorcore'
                'ics_mhs_actorcore.opscore'
                'ics_mhs_actorcore.external'],
)
