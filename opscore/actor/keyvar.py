#!/usr/bin/env python
"""KeyVar and CmdVar

TO DO:
- Add CmdVar
- Add support to KeyVar for refresh commands
"""
import sys
import time
import traceback
import RO.AddCallback
import RO.Constants

__all__ = ["KeyVar"]

class KeyVar(RO.AddCallback.BaseMixin):
    """Container for keyword data.
    
    Callback functions receive three arguments (see Design Note below):
    - valueList: the list of new values (may be an empty list)
    - isCurrent: see the isCurrent method below
    - self: this object
    """
    def __init__(self, actor, key, doPrint=False):
        """Create a KeyVar.
        
        Inputs are:
        - actor: the name of the actor issuing this keyword (string)
        - key: keyword description (opscore.protocols.keys.Key)
        - doPrint: do print data to stdout when set successfully (for debugging)? (boolean)
        """
        self.actor = actor
        self.name = key.name
        self._key = key
        self._typedValues = key.typedValues
        self.doPrint = bool(doPrint)
        self._valueList = ()
        self._isCurrent = False
        self._isGenuine = False
        self._timeStamp = 0
        RO.AddCallback.BaseMixin.__init__(self, defCallNow = True)
    
    def __repr__(self):
        """Return a long str representation of a KeyVar
        """
        return "%s(%r, %r, %s)" % \
            (self.__class__.__name__, self.actor, self.name, self._typedValues)

    def __str__(self):
        """Return a short str representation of a KeyVar
        """
        return "%s(%r, %r)" % \
            (self.__class__.__name__, self.actor, self.name)

    @property
    def valueList(self):
        """Return a copy of the list of values
        """
        return self._valueList[:]
    
    def __getitem__(self, ind):
        """Implement keyVar[ind] to return the specified value from the valueList.
        
        @raise IndexError if ind is out of range
        """
        return self._valueList[ind]

    @property
    def isCurrent(self):
        """Return True if the client is connected to the hub and if
        the actor has output the data since it was last connected to the hub.

        Warning: this value is maintained by a KeyVarDispatcher. Thus it is invalid and should be ignored
        unless you use the KewyordDispatcher or a replacement!
        """
        return self._isCurrent

    @property
    def isGenuine(self):
        """Return True if the value was set from the actor, rather than from a cache.

        Warning: this value is maintained by a KeyVarDispatcher. Thus it is invalid and should be ignored
        unless you use the KewyordDispatcher or a replacement!
        """
        return self._isGenuine
    
    @property
    def timestamp(self):
        """Return the time (in unix seconds, e.g. time.time()) at which value was last set, or 0 if not set.
        """
        return self._timeStamp
    
    def set(self, valueList, isCurrent=True, isGenuine=True, reply=None):
        """Set the values, converting from strings as necessary.

        Inputs:
        - valueList: a list of values (strings or converted to proper data type)
        - isCurrent: new value for isCurrent flag (generally leave this at its default of True)
        - isGenuine: set True if data came from the actor, False if it came from a data cache
        - reply: a parsed Reply object (opscore.protocols.messages.Reply)
        
        @raise RuntimeError if the values cannot be set.
        """
        if not self._typedValues.consume(valueList):
            raise RuntimeError("%s could not parse valueList=%s" % (self, valueList))

        # print to stderr, if requested
        if self.doPrint:
            sys.stderr.write("%s = %r\n" % (self, valueList))

        # apply callbacks, if any
        self._valueList = valueList
        self._timeStamp = time.time()
        self._isCurrent = bool(isCurrent)
        self._isGenuine = bool(isGenuine)
        self.reply = reply
        self._basicDoCallbacks(self)

    def setNotCurrent(self):
        """Clear the isCurrent flag
        
        Note: the flag is set automatically when you call "set", so there is no method to set it.
        """
        self._isCurrent = False
        self._basicDoCallbacks(self)

    def hasRefreshCmd(self):
        """Temporary hack"""
        return False