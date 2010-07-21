#!/usr/bin/env python
"""KeyVar and CmdVar

History:
2009-07-18 ROwen    Changed getRefreshInfo() to a property refreshInfo.
2009-07-19 ROwen    Removed addROWdg and addROWdgSet; added addValueListCallback.
                    Fixed CmdVar timeLimKeyVar handling.
2009-07-20 ROwen    Added getValue property.
2009-07-21 ROwen    Added forUserCmd support to CmdVar.
                    Added "E" message code for error.
2009-07-23 ROwen    Added doCallbacks method to support delayCallbacks in CmdKeyVarDispatcher.
2009-08-25 ROwen    Bug fix: failed to record watched keyVars for cmdVars with forUserCmd set,
                    because it ignored replies with a prefix on the cmdr.
2010-05-26 ROwen    Added __len__ and __iter__ to KeyVar. The former supports len(keyVar); the latter
                    is the recommended way to support iteration (which already worked).
                    Corrected an error in the main doc string for KeyVar.
2010-06-28 ROwen    Bug fix: addValueListCallback startInd argument was ignored (thanks to pychecker).
                    Removed unused import (thanks to pychecker).
"""
import sys
import time
import traceback

import RO.AddCallback
import RO.Constants
import RO.MathUtil

__all__ = ["KeyVar", "AllCodes", "DoneCodes", "FailedCodes", "MsgCodeSeverity"]

AllCodes = "IWE:F!>D"
DoneCodes = ":F!"
FailedCodes = "F!"

# MsgCodeSeverity a dictionary of: message code: associated severity
MsgCodeSeverity = {
    "D": RO.Constants.sevDebug, # debug
    "I": RO.Constants.sevNormal, # information
    ">": RO.Constants.sevNormal, # command queued
    ":": RO.Constants.sevNormal, # command finished
    "W": RO.Constants.sevWarning, # warning
    "E": RO.Constants.sevError, # error
    "F": RO.Constants.sevError, # command failed
    "!": RO.Constants.sevError, # command failed and actor is in trouble
}

class KeyVar(RO.AddCallback.BaseMixin):
    """Container for keyword data.
    
    A keyword variable is iterable and indexable over its values.
    
    Callback functions receive one argument: this object
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
        self.reply = None
        self.key = key
        self._typedValues = key.typedValues
        self.doPrint = bool(doPrint)
        self.valueList = (None,)*self.minVals
        self._isCurrent = False
        self._isGenuine = False
        self._timeStamp = 0
        if self.key.refreshCmd:
            self.refreshActor = self.actor
            self.refreshCmd = key.refreshCmd
        else:
            # have the model set this to a keys command later if not keys.doCache
            self.refreshActor = None
            self.refreshCmd = None
        RO.AddCallback.BaseMixin.__init__(self, defCallNow = True)
    
    def __repr__(self):
        """Return a long str representation of a KeyVar
        """
        return "%s(%r, %s[%s]=%s isCurrent=%s)" % \
            (self.__class__.__name__, self.actor, self.name, self._typedValues, self.valueList, self._isCurrent)

    def __str__(self):
        """Return a short str representation of a KeyVar
        """
        return "%s(%r, %s=%s)" % (self.__class__.__name__, self.actor, self.name, self.valueList)

    def __getitem__(self, ind):
        """Implement keyVar[ind] to return the specified value from the valueList.
        
        @raise IndexError if ind is out of range
        """
        return self.valueList[ind]

    def __iter__(self):
        """Implement for x in keyVar (appears to be redundant, but can't hurt)
        """
        return iter(self.valueList)

    def __len__(self):
        """Implement len(keyVar) to return the number of values
        """
        return len(self.valueList)

    def addValueCallback(self, callFunc, ind=0, cnvFunc=None, callNow=True):
        """Similar to addCallback, but the callback function receives 3 arguments including the specified value.
        
        This is useful for tying objects such as widgets directly to KeyVars.

        Inputs:
        - callFunc: callback function with arguments:
          - value: value at the specified index
          - isCurrent (by name): False if keyVar is not current
          - keyVar (by name): this keyVar
        - ind: index of KeyVar value
        - cnvFunc: a conversion function applied to the value before issuing the callback
        - callNow: if true, execute callFunc immediately, else wait until the keyword is seen
    
        Note: if the KeyVar has a variable number of values and the one specified by ind is not supplied,
        then the callback is not called.
        
        Raise IndexError if not 0 <= ind < maxVals
        """
        RO.MathUtil.checkRange(ind, 0, self.maxVals, "%s ind" % (self,))
        
        if not cnvFunc:
            cnvFunc = lambda x: x
                
        def adapterFunc(keyVar, callFunc=callFunc, ind=ind, cnvFunc=cnvFunc):
            try:
                val = keyVar.valueList[ind]
            except IndexError:
                return
            callFunc(cnvFunc(val), isCurrent=keyVar.isCurrent, keyVar=keyVar)
        self.addCallback(adapterFunc, callNow)

    def addValueListCallback(self, callFuncList, startInd=0, cnvFunc=None, callNow=True):
        """Similar to addCallback, but each callback function receives 3 arguments including the specified value.

        This is useful for tying a collection of objects such as widgets directly to KeyVars.
        addValueListCallback is more efficient than calling addValueCallback
        separately for a collection of functions because the KeyVar has only one callback.

        Inputs:
        - callFuncList: a list of callback functions, each of which has arguments:
          - value: value at the specified index
          - isCurrent (by name): False if keyVar is not current
          - keyVar (by name): this keyVar
        - startInd: index of first KeyVar value
        - cnvFunc: a conversion function applied to each value before issuing the callback
        - callNow: if true, execute callFunc immediately, else wait until the keyword is seen
    
        Note: if the KeyVar has a variable number of values and some are not supplied
        then the associated functions are not called.
        
        Raise IndexError if 0 startInd < 0 or startInd + len(callFuncList) >= maxVals
        """
        endInd = startInd + len(callFuncList) - 1
        RO.MathUtil.checkRange(startInd, 0, None, "%s startInd" % (self,))
        RO.MathUtil.checkRange(endInd, None, self.maxVals, "%s end index" % (self,))
        
        if not cnvFunc:
            cnvFunc = lambda x: x
                
        def adapterFunc(keyVar, callFuncList=callFuncList, startInd=startInd, cnvFunc=cnvFunc):
            for ind, callFunc in enumerate(callFuncList[startInd:]):
                try:
                    val = keyVar.valueList[ind]
                except IndexError:
                    return
                callFunc(cnvFunc(val), isCurrent=keyVar.isCurrent, keyVar=keyVar)
        self.addCallback(adapterFunc, callNow)
    
    def doCallbacks(self):
        """Execute callbacks
        """
        self._basicDoCallbacks(self)

    @property
    def hasRefreshCmd(self):
        """Return True if has a refresh command.
        """
        return bool(self.refreshCmd)
    
    @property
    def refreshInfo(self):
        """Return refreshActor, refreshCmd"""
        return (self.refreshActor, self.refreshCmd)

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
    def maxVals(self):
        """Return the maximum number of (converted) values for this KeyVar, or None if no limit
        """
        return self.key.typedValues.maxVals

    @property
    def minVals(self):
        """Return the minimum number of (converted) values for this KeyVar
        """
        return self.key.typedValues.minVals

    @property
    def timestamp(self):
        """Return the time (in unix seconds, e.g. time.time()) at which value was last set, or 0 if not set.
        """
        return self._timeStamp
    
    def set(self, valueList, isCurrent=True, isGenuine=True, reply=None, doCallbacks=True):
        """Set the values, converting from strings as necessary.

        Inputs:
        - valueList: a list of values (strings or converted to proper data type)
        - isCurrent: new value for isCurrent flag (generally leave this at its default of True)
        - isGenuine: set True if data came from the actor, False if it came from a data cache
        - reply: a parsed Reply object (opscore.protocols.messages.Reply)
        - doCallbacks: if True then issue callbacks
        
        @raise RuntimeError if the values cannot be set.
        """
        valueList = list(valueList)
        if not self._typedValues.consume(valueList):
            raise RuntimeError("%s could not parse valueList=%s" % (self, valueList))

        # print to stderr, if requested
        if self.doPrint:
            sys.stderr.write("%s = %r\n" % (self, valueList))

        # apply callbacks, if any
        self.valueList = tuple(valueList)
        self._timeStamp = time.time()
        self._isCurrent = bool(isCurrent)
        self._isGenuine = bool(isGenuine)
        self.reply = reply
        if doCallbacks:
            self._basicDoCallbacks(self)

    def setNotCurrent(self):
        """Clear the isCurrent flag
        
        Note: the flag is set automatically when you call "set", so there is no method to set it.
        """
        self._isCurrent = False
        self._basicDoCallbacks(self)
     
    def getValue(self, doRaise=True):
        """Return the "value" of the KeyVar, as follows:
        - raise ValueError if keyVar is unknown (valueList contains None) and doRaise is True
        - return True if keyVar always contains 0 elements
        - return keyVar[0] if keyVar always contains 1 element
        - return keyVar.valueList in all other cases
        """
        if doRaise and (None in self.valueList):
            raise ValueError("%s is unknown" % (self,))
        if self.maxVals == 0:
            return True
        if (self.maxVals == 1) and (self.minVals == 1):
            return self.valueList[0]
        return self.valueList

class CmdVar(object):
    """Issue a command via the dispatcher and receive callbacks
    as replies are received.
    """
    def __init__(self,
        cmdStr = "",
        actor = "",
        timeLim = 0,
        description = "",
        callFunc = None,
        callCodes = DoneCodes,
        isRefresh = False,
        timeLimKeyVar = None,
        timeLimKeyInd = 0,
        abortCmdStr = None,
        keyVars = None,
        forUserCmd = None,
    ):
        """
        Inputs:
        - actor: the name of the device which issued the keyword
        - cmdStr: the command; no terminating \n wanted
        - timeLim: maximum time before command expires, in sec; 0 for no limit
        - description: a string describing the command, useful for help systems
        - callFunc: a function to call when the command changes state; see addCallback for details.
        - callCodes: message codes for which to call the callback; see addCallback for details.
        - isRefresh: the command was triggered by a refresh request, else is a user command
        - timeLimKeyVar: a KeyVar specifying a delta-time by which the command must finish
            this KeyVar must be registered with the message dispatcher.
        - timeLimKeyInd: the index of the time limit value in timeLimKeyVar; defaults to 0;
            ignored if timeLimKeyVar is None.
        - abortCmdStr: a command string that will abort the command.
            Sent to the actor if abort is called and if the command is executing.
        - keyVars: a sequence of 0 or more keyword variables to monitor for this command.
            Any data for those variables that arrives IN RESPONSE TO THIS COMMAND is saved
            and can be retrieved using cmdVar.getKeyVarData or cmdVar.getLastKeyVarData.
        - forUserCmd: this command is being sent due to the specified command from a user.
            forUserCmd must have one attribute: cmdr. The command string sent to the hub
            will start with: <forUsreCmd.cmdr>.<dispatcher.name> instead of <dispatcher.name>.
        
        Note: timeLim and timeLimKeyInfo work together as follows:
        - The initial time limit for the command is timeLim
        - If timeLimKeyInfo is seen before timeLim seconds have passed
          then self.maxEndTime is updated with the new value
          
        Also the time limit is a lower limit. The command is guaranteed to expire no sooner than this.
        """
        self.cmdStr = cmdStr
        self.actor = actor
        self.cmdID = None
        self.timeLim = timeLim
        self.description = description
        self.isRefresh = bool(isRefresh)
        self.forUserCmd = forUserCmd
        self._timeLimKeyVar = timeLimKeyVar
        self._timeLimKeyInd = int(timeLimKeyInd)
        if self._timeLimKeyVar:
            # check that value exists and can be cast to a float
            RO.MathUtil.checkRange(self._timeLimKeyInd, 0, self._timeLimKeyVar.minVals, "timeLimKeyInd")
        self.abortCmdStr = abortCmdStr
        # a dictionary of keyVar values; keys is keyVar; value is a list of keyVar.valueList seen for that keyVar
        self.keyVars = keyVars or ()
        self.keyVarDataDict = dict()
        for keyVar in self.keyVars:
            self.keyVarDataDict[keyVar] = []

        self.dispatcher = None # set by dispatcher when it executes the command
        self.replyList = []
        self.lastCode = "Information"
        self.startTime = None
        self.maxEndTime = None

        # the following is a list of (callCodes, callFunc)
        self.callCodesFuncList = []

        if callFunc:
            self.addCallback(callFunc, callCodes)
    
    def abort(self):
        """Abort the command, including:
        - deregister the command from the dispatcher
        - send the abort command (if it exists)
        - set state to failed, calling the appropriate callbacks

        Has no effect if the command was never dispatched or has already ended.
        """
        if self.dispatcher and not self.isDone:
            self.dispatcher.abortCmdByID(self.cmdID)

    def addCallback(self, callFunc, callCodes = DoneCodes):
        """Executes the given function whenever a reply is seen
        for this user with a matching command number

        Inputs:
        - callFunc: a function to call when the command changes state;
            it receives one argument: this CmdVar
        - callCodes: the message codes for which to call the callback;
            a string of one or more message codes (not case sensitive); useful predefined sets include:
                DoneCodes (command finished or failed)
                FailedCodes (command failed)
                AllCodes (all message codes, thus any reply)
        """
        upCallCodes = callCodes.upper()
        self.callCodesFuncList.append((upCallCodes, callFunc))
    
    @property
    def didFail(self):
        """Return True if the command failed, False otherwise.
        """
        return self.lastCode in FailedCodes
    
    @property
    def severity(self):
        """Return severity of most recent message, or RO.Constants.sevNormal if no messages received.
        """
        if not self.lastCode:
            return RO.Constants.sevNormal
        return TypeDict[self.lastCode][1]
    
    def getKeyVarData(self, keyVar):
        """Return a list of data seen for the specified keyword variable, or [] if no data seen.
        
        Inputs:
        - keyVar: the keyword variable for which to return data
    
        Returns a list of data seen for the specified keyVar that was in response to this command,
        in the order received (oldest to most recent). Each entry is the list of values seen for the keyword.
        For example:
            getKeyVarData(keyVar)[-1] is the most recent list of data
                (or an index error if the keyVar was not seen was seen!)
        
        Warning: the return value is NOT a copy. Please do not modify it.
        
        Raises KeyError if the keyVar is not being monitored for this command.
        """
        return self.keyVarDataDict[keyVar]
    
    def getLastKeyVarData(self, keyVar):
        """Return the most recent list of values seen for the specified keyword variable, or None if no data seen.
        
        Inputs:
        - keyVar: the keyword variable for which to return data
        
        Returns the most recent list of values seen for the specified keyVar, or None if no data seen.
        Note: returns None instead of [] because this allows one to tell the difference between
        the keyword value list being itself empty and no data seen for the keyword.

        Warning: the return value is NOT a copy. Please do not modify it.

        Raises KeyError if the keyVar is not being monitored for this command.
        """
        allVals = self.keyVarDataDict[keyVar]
        if not allVals:
            return None
        return allVals[-1]
    
    def handleReply(self, reply):
        """Handle a reply (opscore.protocols.Reply) from the dispatcher.

        Add reply to reply list, call command callbacks and handle cleanup if the reply ends the command.

        Warn and do nothing else if called after the command has finished.
        """
        if self.lastCode in DoneCodes:
            sys.stderr.write("Command %s already finished; no more replies allowed\n" % (self,))
            return
        self.replyList.append(reply)
        msgCode = reply.header.code
        self.lastCode = msgCode
        for callCodes, callFunc in self.callCodesFuncList[:]:
            if msgCode in callCodes:
                try:
                    callFunc(self)
                except Exception:
                    sys.stderr.write("%s callback %s failed\n" % (self, callFunc))
                    traceback.print_exc(file=sys.stderr)
        if self.lastCode in DoneCodes:
            self._cleanup()
    
    @property
    def isDone(self):
        """Return True if the command is finished, False otherwise.
        """
        return self.lastCode in DoneCodes

    @property
    def lastReply(self):
        """Return the last reply object, or None if no replies seen
        """
        if not self.replyList:
            return None
        return self.replyList[-1]

    def removeCallback(self, callFunc, doRaise=True):
        """Delete the callback function.
        Return True if successful, raise error or return False otherwise.

        Inputs:
        - callFunc  callback function to remove
        - doRaise   raise exception if unsuccessful? True by default.
        
        If doRaise true:
        - Raises ValueError if callback not found
        - Raises RuntimeError if executing callbacks when called
        Otherwise returns False in either case.
        """
        for callCodeFunc in self.callCodesFuncList:
            if callFunc == callCodeFunc[1]:
                self.callCodesFuncList.remove(callCodeFunc)
                return True
        if doRaise:
            raise ValueError("Callback %r not found" % callFunc)
        return False
    
    def _timeLimKeyVarCallback(self, keyVar):
        """Handle callback from the time limit keyVar.
        
        Update self.maxEndTime (adding self.timeLim as a margin if timeLim was specified).

        Raises ValueError if the keyword exists but the value is invalid.
        """
        if not self._keyVarIsMine(keyVar):
            return

        newTimeLim = keyVar[self._timeLimKeyInd]
        try:
            newTimeLim = float(newTimeLim)
        except Exception:
            raise ValueError("Invalid timeout value %r in keyword %s for command %s" % (newTimeLim, keyVar, self))
        self.maxEndTime = time.time() + newTimeLim
        if self.timeLim:
            self.maxEndTime += self.timeLim

    def _cleanup(self):
        """Call when command is finished to remove callbacks.
        
        This reduces the chance of memory leaks.
        """
        self.callCodesFuncList = []
        for keyVar in self.keyVars:
            keyVar.removeCallback(self._keyVarCallback, doRaise=False)
        if self._timeLimKeyVar:
            self._timeLimKeyVar.removeCallback(self._timeLimKeyVarCallback, doRaise=False)
    
    def _setStartInfo(self, dispatcher, cmdID):
        """Called by the dispatcher when dispatching the command.
        """
        self.dispatcher = dispatcher
        self.cmdID = cmdID
        self.startTime = time.time()
        if self.timeLim:
            self.maxEndTime = self.startTime + self.timeLim

        for keyVar in self.keyVars:
            keyVar.addCallback(self._keyVarCallback, callNow=False)
        if self._timeLimKeyVar:
            self._timeLimKeyVar.addCallback(self._timeLimKeyVarCallback, callNow=False)

    def _keyVarIsMine(self, keyVar):
        """Return True if keyVar is in response to this command; False otherwise.
        """
        if (not keyVar.isCurrent) or (not keyVar.reply):
            return False
        # note: self.dispatcher should be set, but play it safe
        if not self.dispatcher or not self.dispatcher.replyIsMine(keyVar.reply):
            return False
        if keyVar.reply.header.commandId != self.cmdID:
            return False
        return True

    def _keyVarCallback(self, keyVar):
        """Keyword seen; archive the data.
        """
        if not self._keyVarIsMine(keyVar):
            return
        self.keyVarDataDict[keyVar].append(keyVar.valueList)
    
    def _keyVarID(self, keyVar):
        """Return an ID suitable for use in a dictionary.
        """
        return id(keyVar)
    
    def __repr__(self):
        return "%s(cmdID=%r, actor=%r, cmdStr=%r)" % (self.__class__.__name__, self.cmdID, self.actor, self.cmdStr)
    
    def __str__(self):
        return "%s %r" % (self.actor, self.cmdStr)


class _SetWdgSet(object):
    """KeyVar callback to set a collection of RO.Wdg widgets.
    """
    def __init__(self, wdgSet):
        self.wdgSet = wdgSet
        self.wdgInd = range(len(wdgSet))
    def __call__(self, keyVar):
        isCurrent = keyVar.isCurrent
        for wdg, val in zip(self.wdgSet, keyVar.valueList):
            wdg.set(val, isCurrent=isCurrent, keyVar=keyVar)

class _SetDefaultWdgSet(object):
    """KeyVar callback to set the default of a collection of RO.Wdg widgets.
    """
    def __init__(self, wdgSet):
        self.wdgSet = wdgSet
        self.wdgInd = range(len(wdgSet))
    def __call__(self, keyVar):
        isCurrent = keyVar.isCurrent
        for wdg, val in zip(self.wdgSet, keyVar.valueList):
            wdg.setDefault(val, isCurrent=isCurrent, keyVar=keyVar)
