"""Twisted convenience functions
"""
def cancelTimer(timer):
    """Cancel a Twisted timer, or do nothing if timer is None or inactive.
    """
    if timer != None and timer.active():
        timer.cancel()
