Installation requirements
-------------------------

As of 2018-01, python 2 is deprecated and only python 3 is
supported. python 3.5 is probably OK, but 3.6 may be soon be
required. Only ``POSIX`` systems have been tested.

Additional packages are also required. ``tron_actorcore`` itself requires:
* ``twisted``. We might switch to asyncio since few twisted features
  are used.
* ``ply``. This is essential for command parsing.
* ``future``. We might remove these compatibility shims later.
* ``ruamel-yaml``. For configuration files.
  
For any reasonable actors, you also need:
* ``numpy``. Some actors might require ``scipy``.
* ``cython``. Just at build time.

For any work on ``FITS`` files, both ``astropy`` and ``fitsio``. Sorry
for using both. ``fitsio`` turns out to be needs for the many-HDU NIR
ramp files.

