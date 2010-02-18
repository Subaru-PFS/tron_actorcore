"""Twisted one-shot timer

Requires a running twisted reactor.
"""
import twisted.internet.reactor

_reactor = twisted.internet.reactor

class Timer(object):
    """A restartable one-shot timer
    """
    def __init__(self, *args, **keyArgs):
        """Start or set up a one-shot timer

        Inputs:
        - sec: interval, in seconds (float); if omitted then the timer is not started
        - callFunc: function to call when timer fires
        *args: arguments for callFunc
        **keyArgs: keyword arguments for callFunc
        
        Warning: sec and callFunc are positional; they may not be specified by name
        """
        if args:
            self._timer = _reactor.callLater(*args, **keyArgs)
        else:
            self._timer = None
    
    def start(self, sec, callFunc, *args, **keyArgs):
        """Start or restart the timer, cancelling a pending timer if present
        
        Inputs:
        - sec: interval, in seconds (float)
        - callFunc: function to call when timer fires
        *args: arguments for callFunc
        **keyArgs: keyword arguments for callFunc
        """
        self.cancel()
        self._timer = _reactor.callLater(sec, callFunc, *args, **keyArgs)

    def cancel(self):
        """Cancel the timer; a no-op if the timer is not active"""
        if self._timer != None and self._timer.active():
            self._timer.cancel()

    def active(self):
        """Return True if the timer is active"""
        return self._timer.active()
