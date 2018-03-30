Sending commands to other actors
================================

There are two ways to send commands to other actors: synchronously,
where your actor thread blocks until the sent command finishes or
times out, and asynchronously, where the results of the sent command
get sent to a callback function.

All examples will be from ``....Cmd.py`` object methods. This will
provide a ``self.actor`` object, which is needed to send commands. Also
a ``cmd`` object, as passed in to the methods in the ``Commands/`` files.

Synchronous commands
--------------------

In most cases, you will only need to send a command string to an actor,
with an expected execution time:

.. code-block:: python

   def centroid(self, cmd):
       cmdString = "centroid expTime=%0.1f" % (self.expTime)
       cmdVar = self.actor.cmdr.call(actor='mcs',
                                     cmdStr=cmdString,
                                     forUserCmd=cmd,
                                     timeLim=self.expTime+5.0)
       if cmdVar.didFail:
           cmd.fail('text="Failed to expose with %s"' %
                    (cmdString)
           return

       self.handleCentroids()
       cmd.finish()

If you have a ``Command`` object (``cmd``, here) available and are calling
your command on its behalf, please do pass it in as shown:
``forUserCmd=cmd``. That help associate the two commands for various
administrative purposes.

Asynchronous commands
---------------------

