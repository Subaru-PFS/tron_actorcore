# Tron Actorcore

Common code base for the PFS MHS (Message Handling System) actor ecosystem.

This repository packages and distributes four Python modules that are commonly used by PFS actors and tooling:

- actorcore — base classes and utilities for writing PFS actors.
- opscore — protocol, actor framework utilities, and supporting code.
- pfscore — PFS-specific shared utilities (e.g., Gen2 helpers).
- RO (and vendored RO-3.6.9) — UI/utility code historically used within the ecosystem.

## Installation

Important: `ics_utils` is not installed as part of a pip install of this package due to a circular dependency. You must
install it manually when needed.

Preferred method (recommended):

- pip install ics_utils

Installing ics_utils will automatically pull in pfs-tron-actorcore as a dependency.

Alternative installation paths:

- pip install pfs-tron-actorcore
- pip install ics_utils # must be installed separately because it is not auto-installed here

Python version: requires Python 3.11 or newer.

## Windows and MacOS

Some RO-related extras for specific platforms are available via the `ro_extras` extra.

- pip install "pfs-tron-actorcore[ro_extras]"
