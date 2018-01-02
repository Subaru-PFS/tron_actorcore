#!/usr/bin/env python
"""Code to run scripts that can wait for various things
without messing up the main event loop
(and thus starving the rest of your program).

ScriptRunner allows your script to wait for the following:
- wait for a given time interval using: yield waitMS(...)
- run a slow computation as a background thread using waitThread
- run a command via the keyword dispatcher using waitCmd
- run multiple commands at the same time:
  - start each command with startCmd,
  - wait for one or more commands to finish using waitCmdVars
- wait for a keyword variable to be set using waitKeyVar
- wait for a subscript by yielding it (i.e. yield subscript(...))
  note that the subscript must contain a yield for this to work;
  if it has no yield then just call it directly

An example is given as the test code at the end.
  
Code comments:
- Wait functions use a class to do all the work. This standardizes
  some tricky internals (such as registering and deregistering
  cancel functions) and allows the class to easily keep track
  of private internal data.
- The wait class is created but is not explicitly kept around.
  Why doesn't it immediately vanish? Because the wait class
  registers a method as a completion callback and usual
  another method as a cancel function. As long as somebody
  has a pointer to an method then the instance is kept alive.
- waitThread originally relied on generating an event when the script ended.
  Unfortunately, that proved unreliable; if the thread was very short,
  it could actually start trying to continue before the current
  iteration of the generator was finished! I'm surprised that was
  possible (I expected the event to get queued), but in any case
  it was bad news. The current scheme is a kludge -- poll the thread.
  I hope I can figure out something better.

History:
2004-08-12 ROwen
2004-09-10 ROwen    Modified for RO.Wdg.Constants->RO.Constants.
                    Bug fix: _WaitMS cancel used afterID instead of self.afterID.
                    Bug fix: test for resume while wait callback pending was broken,
                    leading to false "You forgot the 'yield'" errors.
2004-10-01 ROwen    Bug fix: waitKeyVar was totally broken.
2004-10-08 ROwen    Bug fix: waitThread could fail if the thread was too short.
2004-12-16 ROwen    Added a debug mode that prints diagnostics to stdout
                    and does not wait for commands or keyword variables.
2005-01-05 ROwen    showMsg: changed level to severity.
2005-06-16 ROwen    Changed default cmdStatusBar from statusBar to no bar.
2005-06-24 ROwen    Changed to use new CmdVar.lastReply instead of .replies.
2005-08-22 ROwen    Clarified _WaitCmdVars.getState() doc string.
2006-03-09 ROwen    Added scriptClass argument to ScriptRunner.
2006-03-28 ROwen    Modified to allow scripts to call subscripts.
2006-04-24 ROwen    Improved error handling in _continue.
                    Bug fixes to debug mode:
                    - waitCmd miscomputed iterID
                    - startCmd dispatched commands
2006-11-02 ROwen    Added checkFail argument to waitCmd and waitCmdVars methods.
                    waitCmd now returns the cmdVar in sr.value.
                    Added keyVars argument to startCmd and waitCmd.
2006-11-13 ROwen    Added waitUser and resumeUser methods.
2006-12-12 ROwen    Bug fix: start did not initialize waitUser instance vars.
                    Added initVars method to centralize initialization.
2008-04-21 ROwen    Improved debug mode output:
                    - showMsg prints messages
                    - _setState prints requested state
                    - _end prints the end function
                    Added debugPrint method to simplify handling unicode errors.
2008-04-24 ROwen    Bug fix: waitKeyVar referenced a nonexistent variable in non-debug mode.
2008-04-29 ROwen    Fixed reporting of exceptions that contain unicode arguments.
2008-06-26 ROwen    Improved documentation for abortCmdStr and keyVars arguments to waitCmd
2010-02-17 ROwen    Copied from RO to opscore and adapted to work with opscore.
                    Changed state constants from module constants to class constants
                    and from integers to strings.
2010-03-11 ROwen    Fixed a few instances of obsolete keyVar.get() and keyVar.isCurrent().
2010-06-28 ROwen    Made _WaitBase a modern class (thanks to pychecker).
                    Removed unused and broken internal method _waitEndFunc (thanks to pychecker).
2010-11-19 ROwen    Bug fix: FailCodes -> FailedCodes.
2011-05-04 ROwen    Bug fix: startCmd debug mode was broken; it called nonexistent dispatcher.makeMsgDict
                    instead of dispatcher.makeReply and cmdVar.reply instead of cmdVar.handleReply.
2012-06-01 ROwen    Use best effort to remove callbacks during cleanup, instead of raising an exception on failure.
                    Modified _WaitCmdVars to not try to register callbacks on commands that are finished,
                    and to not try to remove callbacks from CmdVars that are done.
"""
from __future__ import print_function
from __future__ import absolute_import
import sys
import threading
import Queue
import traceback
import RO.AddCallback
import RO.Constants
import RO.SeqUtil
import RO.StringUtil
from opscore.utility.timer import Timer
from . import keyvar

_DebugState = False

# internal constants
_PollDelaySec = 0.1 # polling interval for threads (sec)

# a list of possible keywords that hold reasons for a command failure
# in the order in which they are checked
_ErrKeys = ("text",)

class _Blank(object):
    def __init__(self):
        object.__init__(self)

class ScriptError (RuntimeError):
    """Use to raise exceptions in your script
    when you don't want a traceback.
    """
    pass

class ScriptRunner(RO.AddCallback.BaseMixin):
    """Execute a script.

    Allows waiting for various things without messing up the main event loop.
    """
    # state constants
    Ready = "Ready"
    Paused = "Paused"
    Running = "Running"
    Done = "Done"
    Cancelled = "Cancelled"
    Failed = "Failed"
    _AllStates = (Ready, Paused, Running, Done, Cancelled, Failed)
    _RunningStates = (Paused, Running)
    _DoneStates = (Done, Cancelled, Failed)
    _FailedStates = (Cancelled, Failed)
    def __init__(self,
        name,
        runFunc = None,
        scriptClass = None,
        dispatcher = None,
        master = None,
        initFunc = None,
        endFunc = None,
        stateFunc = None,
        startNow = False,
        statusBar = None,
        cmdStatusBar = None,
        debug = False,
    ):
        """Create a ScriptRunner
        
        Inputs:
        - name          script name; used to report status
        - runFunc       the main script function; executed whenever
                        the start button is pressed
        - scriptClass   a class with a run method and an optional end method;
                        if specified, runFunc, initFunc and endFunc may not be specified.
        - dispatcher    keyword dispatcher (opscore.actor.CmdKeyVarDispatcher);
                        required to use wait methods and startCmd.
        - master        master Tk widget; your script may grid or pack objects into this;
                        may be None for scripts that do not have widgets.
        - initFunc      function to call ONCE when the ScriptRunner is constructed
        - endFunc       function to call when runFunc ends for any reason
                        (finishes, fails or is cancelled); used for cleanup
        - stateFunc     function to call when the ScriptRunner changes state
        - startNow      if True, starts executing the script immediately
                        instead of waiting for user to call start.
        - statusBar     status bar, if available. Used by showMsg
        - cmdStatusBar  command status bar, if available.
                        Used to show the status of executing commands.
                        May be the same as statusBar.
        - debug         if True, startCmd and wait... print diagnostic messages to stdout
                        and there is no waiting for commands or keyword variables. Thus:
                        - waitCmd and waitCmdVars return success immediately
                        - waitKeyVar returns defVal (or None if not specified) immediately
    
        All functions (runFunc, initFunc, endFunc and stateFunc) receive one argument: sr,
        this ScriptRunner object. The functions can pass information using sr.globals,
        an initially empty object (to which you can add instance variables and set or read them).
        
        Only runFunc is allowed to call sr methods that wait.
        The other functions may only run non-waiting code.
    
        WARNING: when runFunc calls any of the ScriptRunner methods that wait,
        IT MUST YIELD THE RESULT, as in:
            def runFunc(sr):
                ...
                yield sr.waitMS(500)
                ...
        All such methods are marked "yield required".
        
        If you forget to yield, your script will not wait. Your script will then halt
        with an error message when it calls the next ScriptRunner method that involves waiting
        (but by the time it gets that far it may have done some strange things).
        
        If your script yields when it should not, it will simply halt.
        """
        if scriptClass:
            if runFunc or initFunc or endFunc:
                raise ValueError("Cannot specify runFunc, initFunc or endFunc with scriptClass")
            if not hasattr(scriptClass, "run"):
                raise ValueError("scriptClass=%r has no run method" % scriptClass)
        elif runFunc == None:
            raise ValueError("Must specify runFunc or scriptClass")
        elif not callable(runFunc):
            raise ValueError("runFunc=%r not callable" % (runFunc,))

        self.runFunc = runFunc
        self.name = name
        self.dispatcher = dispatcher
        self.master = master
        self.initFunc = initFunc
        self.endFunc = endFunc
        self.debug = bool(debug)
        self._statusBar = statusBar
        self._cmdStatusBar = cmdStatusBar
        
        # useful constant for script writers
        self.ScriptError = ScriptError
        
        RO.AddCallback.BaseMixin.__init__(self)

        self.globals = _Blank()
        
        self.initVars()

        if stateFunc:
            self.addCallback(stateFunc)
        
        # initialize, as appropriate
        if scriptClass:
            self.scriptObj = scriptClass(self)
            self.runFunc = self.scriptObj.run
            self.endFunc = getattr(self.scriptObj, "end", None)
        elif self.initFunc:
            res = self.initFunc(self)
            if hasattr(res, "next"):
                raise RuntimeError("init function tried to wait")
        
        if startNow:
            self.start()
    
    # methods for starting, pausing and aborting script
    # and for getting the current state of execution.
    
    def cancel(self):
        """Cancel the script.

        The script will not actually halt until the next
        waitXXX or doXXX method is called, but this should
        occur quickly.
        """
        if self.isExecuting:
            self._setState(self.Cancelled, "")
    
    def debugPrint(self, msgStr):
        """Print the message to stdout if in debug mode.
        Handles unicode as best it can.
        """
        if not self.debug:
            return
        try:
            print(msgStr)
        except (TypeError, ValueError) as e:
            print(repr(msgStr))

    @property
    def fullState(self):
        """Returns the current state as a tuple:
        - state: a string value; should match a named state constant
        - reason: the reason for the state ("" if none)
        """
        state, reason = self._state, self._reason
        return (state, reason)
    
    @property
    def state(self):
        """Return the current state as a string.
        See the state constants defined in this class.
        See also fullState.
        """
        return self._state
    
    def initVars(self):
        """Initialize variables.
        Call at construction and when starting a new run.
        """
        self._cancelFuncs = []
        self._endingState = None
        self._state = self.Ready
        self._reason = ""
        self._iterID = [0]
        self._iterStack = []
        self._waiting = False # set when waiting for a callback
        self._userWaitID = None
        self.value = None
        
    @property
    def didFail(self):
        """Return True if script aborted or failed.
        
        Note: may not be fully ended (there may be cleanup to do and callbacks to call).
        """
        return self._endingState in self._FailedStates

    @property
    def isDone(self):
        """Return True if script is finished, successfully or otherwise.

        Note: may not be fully ended (there may be cleanup to do and callbacks to call).
        """
        return self._state in self._DoneStates
    
    @property
    def isExecuting(self):
        """Returns True if script is running or paused."""
        return self._state in self._RunningStates

    @property
    def isPaused(self):
        """Return True if script is paused."""
        return self._state == self.Paused
    
    def pause(self):
        """Pause execution.
        
        Note that the script must be waiting for something when the pause occurs
        (because that's when the GUI will be freed up to get the request to pause).
        If the thing being waited for fails then the script will fail (thus going
        from Paused to Failed with no user interation).

        Has no effect unless the script is running.
        """
        self._printState("pause")
        if not self._state == self.Running:
            return

        self._setState(self.Paused)
    
    def resume(self):
        """Resume execution after a pause.

        Has no effect if not paused.
        """
        self._printState("resume")
        if not self._state == self.Paused:
            return

        self._setState(self.Running)
        if not self._waiting:
            self._continue(self._iterID)

    def resumeUser(self):
        """Resume execution from waitUser
        """
        if self._userWaitID == None:
            raise RuntimeError("Not in user wait mode")
            
        iterID = self._userWaitID
        self._userWaitID = None
        self._continue(iterID)

    def start(self):
        """Start executing runFunc.
        
        If already running, raises RuntimeError
        """
        if self.isExecuting:
            raise RuntimeError("already executing")
    
        if self._statusBar:
            self._statusBar.setMsg("")
        if self._cmdStatusBar:
            self._cmdStatusBar.setMsg("")
        
        self.initVars()
    
        self._iterID = [0]
        self._iterStack = []
        self._setState(self.Running)
        self._continue(self._iterID)
    
    # methods for use in scripts
    # with few exceptions all wait for something
    # and thus require a "yield"
    
    def getKeyVar(self,
        keyVar,
        ind=0,
        defVal=Exception,
    ):
        """Return the current value of keyVar.
        See also waitKeyVar, which can wait for a value.

        Note: if you want to be sure the keyword data was in response to a particular command
        that you sent, then use the keyVars argument of startCmd or waitCmd instead.

        Do not use yield because it does not wait for anything.

        Inputs:
        - keyVar    keyword variable
        - ind       which value is wanted? (None for all values)
        - defVal    value to return if value cannot be determined
                    (if omitted, the script halts)
        """
        if self.debug:
            argList = ["keyVar=%s" % (keyVar,)]
            if ind != 0:
                argList.append("ind=%s" % (ind,))
            if defVal != Exception:
                argList.append("defVal=%r" % (defVal,))
            if defVal == Exception:
                defVal = None

        if keyVar.isCurrent:
            if ind != None:
                retVal = keyVar[ind]
            else:
                retVal = keyVar.valueList
        else:
            if defVal==Exception:
                raise ScriptError("Value of %s invalid" % (keyVar,))
            else:
                retVal = defVal

        if self.debug: # else argList does not exist
            self.debugPrint("getKeyVar(%s); returning %r" % (", ".join(argList), retVal))
        return retVal
    
    def showMsg(self, msg, severity=RO.Constants.sevNormal):
        """Display a message--on the status bar, if available,
        else sys.stdout.

        Do not use yield because it does not wait for anything.
        
        Inputs:
        - msg: string to display, without a final \n
        - severity: one of RO.Constants.sevNormal (default), sevWarning or sevError
        """
        if self._statusBar:
            self._statusBar.setMsg(msg, severity)
            self.debugPrint(msg)
        else:
            print(msg)
    
    def startCmd(self,
        actor="",
        cmdStr = "",
        timeLim = 0,
        callFunc = None,
        callCodes = keyvar.DoneCodes,
        timeLimKeyVar = None,
        timeLimKeyInd = 0,
        abortCmdStr = None,
        keyVars = None,
        checkFail = True,
    ):
        """Start a command using the same arguments as waitCmd (which see).
        Returns a command variable that you can wait for using waitCmdVars.

        Do not use yield because it does not wait for anything.
        
        Inputs: same as waitCmd, which see.
        
        Returns a command variable which you can wait for using waitCmdVars.
        """
        cmdVar = keyvar.CmdVar(
            actor=actor,
            cmdStr = cmdStr,
            timeLim = timeLim,
            callFunc = callFunc,
            callCodes = callCodes,
            timeLimKeyVar = timeLimKeyVar,
            timeLimKeyInd = timeLimKeyInd,
            abortCmdStr = abortCmdStr,
            keyVars = keyVars,
        )

        if checkFail:
            cmdVar.addCallback(
                callFunc = self._cmdFailCallback,
                callCodes = keyvar.FailedCodes,
            )
        if self.debug:
            argList = ["actor=%r, cmdStr=%r" % (actor, cmdStr)]
            if timeLim != 0:
                argList.append("timeLim=%s" % (timeLim,))
            if callFunc != None:
                argList.append("callFunc=%r" % (callFunc,))
            if callCodes != keyvar.DoneCodes:
                argList.append("callCodes=%r" % (callCodes,))
            if timeLimKeyVar != None:
                argList.append("timeLimKeyVar=%r" % (timeLimKeyVar,))
            if abortCmdStr != None:
                argList.append("abortCmdStr=%r" % (abortCmdStr,))
            if checkFail != True:
                argList.append("checkFail=%r" % (checkFail,))
            self.debugPrint("startCmd(%s)" % ", ".join(argList))

            self._showCmdMsg("%s started" % cmdStr)
            

            # set up command completion callback
            def endCmd(self=self, cmdVar=cmdVar):
                endReply = self.dispatcher.makeReply(
                    cmdr = None,
                    cmdID = cmdVar.cmdID,
                    actor = cmdVar.actor,
                    msgCode = ":",
                )
                cmdVar.handleReply(endReply)
                self._showCmdMsg("%s finished" % cmdVar.cmdStr)
            Timer(1.0, endCmd)

        else:
            if self._cmdStatusBar:
                self._cmdStatusBar.doCmd(cmdVar)
            else:
                self.dispatcher.executeCmd(cmdVar)
                
        return cmdVar
    
    def waitCmd(self,
        actor="",
        cmdStr = "",
        timeLim = 0,
        callFunc=None,
        callCodes = keyvar.DoneCodes,
        timeLimKeyVar = None,
        timeLimKeyInd = 0,
        abortCmdStr = None,
        keyVars = None,
        checkFail = True,
    ):
        """Start a command and wait for it to finish.
        Returns the command variable (an opscore.actor.CmdVar) in sr.value.

        A yield is required.
        
        Inputs:
        - actor: the name of the device to command
        - cmdStr: the command (without a terminating \n)
        - timeLim: maximum time before command expires, in sec; 0 for no limit
        - callFunc: a function to call when the command changes state;
            see below for details.
        - callCodes: the message types for which to call the callback;
            a string of one or more choices; see keyvar.TypeDict for the choices;
            useful constants include DoneTypes (command finished or failed)
            and AllTypes (all message types, thus any reply).
            Not case sensitive (the string you supply will be lowercased).
        - timeLimKeyVar: a keyword (opscore.actor.KeyVar) whose value at index timeLimKeyInd
            is used as a time within which the command must finish (in seconds).
        - timeLimKeyInd: see timeLimKeyVar; ignored if timeLimKeyVar omitted.
        - abortCmdStr: a command string that will abort the command. This string is sent to the actor
            if the command is aborted, e.g. if the script is cancelled while the command is executing.
        - keyVars: a sequence of 0 or more keyword variables (opscore.actor.KeyVar) to monitor.
            Any data for those variables that arrives IN RESPONSE TO THIS COMMAND is saved in the cmdVar
            returned in sr.value and can be retrieved using cmdVar.getKeyVarData or cmdVar.getLastKeyVarData
        - checkFail: check for command failure?
            if True (the default) command failure will halt your script

        The callback receives one argument: the command variable (an opscore.actor.CmdVar).
        
        Note: timeLim and timeLimKeyVar work together as follows:
        - The initial time limit for the command is timeLim
        - If timeLimKeyVar is seen before timeLim seconds have passed
          then self.maxEndTime is updated with the new value
          
        Also the time limit is a lower limit. The command is guaranteed to
        expire no sooner than this but it may take a second longer.
        """
        self._waitCheck(setWait = False)
        
        self.debugPrint("waitCmd calling startCmd")

        cmdVar = self.startCmd (
            actor = actor,
            cmdStr = cmdStr,
            timeLim = timeLim,
            callFunc = callFunc,
            callCodes = callCodes,
            timeLimKeyVar = timeLimKeyVar,
            timeLimKeyInd = timeLimKeyInd,
            abortCmdStr = abortCmdStr,
            keyVars = keyVars,
            checkFail = False,
        )
        
        self.waitCmdVars(cmdVar, checkFail=checkFail, retVal=cmdVar)
        
    def waitCmdVars(self, cmdVars, checkFail=True, retVal=None):
        """Wait for one or more command variables to finish.
        Command variables are the objects returned by startCmd.
        
        A yield is required.
        
        Returns successfully if all commands succeed.
        Fails as soon as any command fails.
        
        Inputs:
        - one or more command variables (keyvar.CmdVar objects)
        - checkFail: check for command failure?
            if True (the default) command failure will halt your script
        - retVal: value to return at the end; defaults to None
        """
        _WaitCmdVars(self, cmdVars, checkFail=checkFail, retVal=retVal)
        
    def waitKeyVar(self,
        keyVar,
        ind=0,
        defVal=Exception,
        waitNext=False,
    ):
        """Get the value of keyVar in self.value.
        If it is currently unknown or if waitNext is true,
        wait for the variable to be updated.
        See also getKeyVar (which does not wait).

        A yield is required.
        
        Inputs:
        - keyVar    keyword variable
        - ind       index of desired value (None for all values)
        - defVal    value to return if value cannot be determined; if Exception, the script halts
        - waitNext  if True, ignore the current value and wait for the next transition.
        """
        _WaitKeyVar(
            scriptRunner = self,
            keyVar = keyVar,
            ind = ind,
            defVal = defVal,
            waitNext = waitNext,
        )

    def waitMS(self, msec):
        """Waits for msec milliseconds.
        
        A yield is required.

        Inputs:
        - msec  number of milliseconds to pause
        """
        self.debugPrint("waitMS(msec=%s)" % (msec,))

        _WaitMS(self, msec)
    
    def waitThread(self, func, *args, **kargs):
        """Run func as a background thread, waits for completion
        and sets self.value = the result of that function call.

        A yield is required.
        
        Warning: func must NOT interact with Tkinter widgets or variables
        (not even reading them) because Tkinter is not thread-safe.
        (The only thing I'm sure a background thread can safely do with Tkinter
        is generate an event, a technique that is used to detect end of thread).
        """
        self.debugPrint("waitThread(func=%r, args=%s, keyArgs=%s)" % (func, args, kargs))

        _WaitThread(self, func, *args, **kargs)
    
    def waitUser(self):
        """Wait until resumeUser called.
        
        Typically used if waiting for user input
        but can be used for any external trigger.
        """
        self._waitCheck(setWait=True)

        if self._userWaitID != None:
            raise RuntimeError("Already in user wait mode")
            
        self._userWaitID = self._getNextID()

    def _cmdFailCallback(self, cmdVar):
        """Use as a callback for when an asynchronous command fails.
        """
#       print "ScriptRunner._cmdFailCallback(%r)" % (cmdVar,)
        if not cmdVar.didFail:
            errMsg = "Bug! RO.ScriptRunner._cmdFail(%r) called for non-failed command" % (cmdVar,)
            raise RuntimeError(errMsg)
        MaxLen = 10
        if len(cmdVar.cmdStr) > MaxLen:
            cmdDescr = "%s %s..." % (cmdVar.actor, cmdVar.cmdStr[0:MaxLen])
        else:
            cmdDescr = "%s %s" % (cmdVar.actor, cmdVar.cmdStr)
        lastReply = cmdVar.lastReply
        if lastReply:
            for keyword in lastReply.keywords:
                if keyword.name.lower() in _ErrKeys:
                    reason = keyword.values[0] or "?"
                    break
            else:
                reason = lastReply.string or "?"
        else:
            reason = "?"
        self._setState(self.Failed, reason="%s failed: %s" % (cmdDescr, reason))
    
    def _continue(self, iterID, val=None):
        """Continue executing the script.
        
        Inputs:
        - iterID: ID of iterator that is continuing
        - val: self.value is set to val
        """
        self._printState("_continue(%r, %r)" % (iterID, val))
        if not self.isExecuting:
            raise RuntimeError('%s: bug! _continue called but script not executing' % (self,))
        
        try:
            if iterID != self._iterID:
                #print "Warning: _continue called with iterID=%s; expected %s" % (iterID, self._iterID)
                raise RuntimeError("%s: bug! _continue called with bad id; got %r, expected %r" % (self, iterID, self._iterID))
    
            self.value = val
            
            self._waiting = False
            
            if self.isPaused:
                #print "_continue: still paused"
                return
        
            if not self._iterStack:
                # just started; call run function,
                # and if it's an iterator, put it on the stack
                res = self.runFunc(self)
                if not hasattr(res, "next"):
                    # function was a function, not a generator; all done
                    self._setState(self.Done)
                    return

                self._iterStack = [res]
            
            self._printState("_continue: before iteration")
            self._state = self.Running
            possIter = next(self._iterStack[-1])
            if hasattr(possIter, "next"):
                self._iterStack.append(possIter)
                self._iterID = self._getNextID(addLevel = True)
#               print "Iteration yielded an iterator"
                self._continue(self._iterID)
            else:
                self._iterID = self._getNextID()
            
            self._printState("_continue: after iteration")

        except StopIteration:
#           print "StopIteration seen in _continue"
            self._iterStack.pop(-1)
            if not self._iterStack:
                self._setState(self.Done)
            else:
                self._continue(self._iterID, val=self.value)
        except KeyboardInterrupt:
            self._setState(self.Cancelled, "keyboard interrupt")
        except SystemExit:
            self.__del__()
            sys.exit(0)
        except ScriptError as e:
            self._setState(self.Failed, RO.StringUtil.strFromException(e))
        except Exception as e:
            traceback.print_exc(file=sys.stderr)
            self._setState(self.Failed, RO.StringUtil.strFromException(e))
    
    def _printState(self, prefix):
        """Print the state at various times.
        Ignored unless _DebugState or self.debug true.
        """
        if _DebugState:
            print("Script %s: %s: state=%s, iterID=%s, waiting=%s, iterStack depth=%s" % \
                (self.name, prefix, self._state, self._iterID, self._waiting, len(self._iterStack)))

    def _showCmdMsg(self, msg, severity=RO.Constants.sevNormal):
        """Display a message--on the command status bar, if available,
        else sys.stdout.

        Do not use yield because it does not wait for anything.
        
        Inputs:
        - msg: string to display, without a final \n
        - severity: one of RO.Constants.sevNormal (default), sevWarning or sevError
        """
        if self._cmdStatusBar:
            self._cmdStatusBar.setMsg(msg, severity)
        else:
            print(msg)
    
    def __del__(self):
        """Called just before the object is deleted.
        Deletes any state callbacks and then cancels script execution.
        """
        self._callbacks = []
        self.cancel()
    
    def _end(self):
        """Call the end function (if any).
        """
        # Warning: this code must not execute _setState or __del__
        # to avoid infinite loops. It also need not execute _cancelFuncs.
        if self.endFunc:
            self.debugPrint("ScriptRunner._end: calling end function")
            try:
                res = self.endFunc(self)
                if hasattr(res, "next"):
                    self._state = self.Failed
                    self._reason = "endFunc tried to wait"
            except KeyboardInterrupt:
                self._state = self.Cancelled
                self._reason = "keyboard interrupt"
            except SystemExit:
                raise
            except Exception as e:
                self._state = self.Failed
                self._reason = "endFunc failed: %s" % (RO.StringUtil.strFromException(e),)
                traceback.print_exc(file=sys.stderr)
        else:
            self.debugPrint("ScriptRunner._end: no end function to call")
    
    def _getNextID(self, addLevel=False):
        """Return the next iterator ID"""
        self._printState("_getNextID(addLevel=%s)" % (addLevel,))
        newID = self._iterID[:]
        if addLevel:
            newID += [0]
        else:
            newID[-1] = (newID[-1] + 1) % 10000
        return newID
    
    def _setState(self, newState, reason=None):
        """Update the state of the script runner.

        If the new state is Cancelled or Failed
        then any existing cancel function is called
        to abort outstanding callbacks.
        
        If the state is unknown, then the command is rejected.
        """
        self._printState("_setState(%r, %r)" % (newState, reason))
        if newState not in self._AllStates:
            raise RuntimeError("Unknown state", newState)
        
        # if ending, clean up appropriately
        if self.isExecuting and newState in self._DoneStates:
            self._endingState = newState
            # if aborting and a cancel function exists, call it
            if newState in self._FailedStates:
                for func in self._cancelFuncs:
#                   print "%s _setState calling cancel function %r" % (self, func)
                    func()
            self._cancelFuncs = []
            self._end()
            
        self._state = newState
        if reason != None:
            self._reason = reason
        self._doCallbacks()
    
    def __str__(self):
        """String representation of script"""
        return "script %s" % (self.name,)
    
    def _waitCheck(self, setWait=False):
        """Verifies that the script runner is running and not already waiting
        (as can easily happen if the script is missing a "yield").
        
        Call at the beginning of every waitXXX method.
        
        Inputs:
        - setWait: if True, sets the _waiting flag True
        """
        if self._state != self.Running:
            raise RuntimeError("Tried to wait when not running")
        
        if self._waiting:
            raise RuntimeError("Already waiting; did you forget the 'yield' when calling a ScriptRunner method?")
        
        if setWait:
            self._waiting = True


class _WaitBase(object):
    """Base class for waiting.
    Handles verifying iterID, registering the termination function,
    registering and unregistering the cancel function, etc.
    """
    def __init__(self, scriptRunner):
        scriptRunner._printState("%s init" % (self.__class__.__name__))
        scriptRunner._waitCheck(setWait = True)
        self.scriptRunner = scriptRunner
        self._iterID = scriptRunner._getNextID()
        self.scriptRunner._cancelFuncs.append(self.cancelWait)

    def cancelWait(self):
        """Call to cancel waiting.
        Perform necessary cleanup but do not set state.
        Subclasses can override and should usually call cleanup.
        """
        self.cleanup()
    
    def fail(self, reason):
        """Call if waiting fails.
        """
        # report failure; this causes the scriptRunner to call
        # all pending cancelWait functions, so don't do that here
        self.scriptRunner._setState(self.Failed, reason)
    
    def cleanup(self):
        """Called when ending for any reason
        (unless overridden cancelWait does not call cleanup).
        """
        pass
    
    def _continue(self, val=None):
        """Call to resume execution."""
        self.cleanup()
        try:
            self.scriptRunner._cancelFuncs.remove(self.cancelWait)
        except ValueError:
            raise RuntimeError("Cancel function missing; did you forgot the 'yield' when calling a ScriptRunner method?")
        if self.scriptRunner.debug and val != None:
            print("wait returns %r" % (val,))
        self.scriptRunner._continue(self._iterID, val)


class _WaitMS(_WaitBase):
    def __init__(self, scriptRunner, msec):
        self._waitTimer = Timer()
        _WaitBase.__init__(self, scriptRunner)
        self._waitTimer.start(msec / 1000.0, self._continue)
    
    def cancelWait(self):
        self._waitTimer.cancel()


class _WaitCmdVars(_WaitBase):
    """Wait for one or more command variables to finish.
    
    Inputs:
    - scriptRunner: the script runner
    - one or more command variables (keyvar.CmdVar objects)
    - checkFail: check for command failure?
        if True (the default) command failure will halt your script
    - retVal: the value to return at the end (in scriptRunner.value)
    """
    def __init__(self, scriptRunner, cmdVars, checkFail=True, retVal=None):
        self.cmdVars = RO.SeqUtil.asSequence(cmdVars)
        self.checkFail = bool(checkFail)
        self.retVal = retVal
        self.addedCallback = False
        _WaitBase.__init__(self, scriptRunner)

        if self.state[0] != 0:
            # no need to wait; commands are already done or one has failed
            # schedule a callback for asap
#            print "_WaitCmdVars: no need to wait"
            Timer(0.001, self.varCallback)
        else:
            # need to wait; add self as callback to each cmdVar
            # and remove self.scriptRunner._cmdFailCallback if present
            for cmdVar in self.cmdVars:
                if not cmdVar.isDone:
                    cmdVar.removeCallback(self.scriptRunner._cmdFailCallback, doRaise=False)
                    cmdVar.addCallback(self.varCallback)
                    self.addedCallback = True

    @property
    def state(self):
        """Return one of:
        - (-1, failedCmdVar) if a command has failed and checkFail True
        - (1, None) if all commands are done (and possibly failed if checkFail False)
        - (0, None) not finished yet
        Note that state[0] is logically True if done waiting.
        """
        allDone = 1
        for cmdVar in self.cmdVars:
            if cmdVar.isDone:
                if cmdVar.didFail and self.checkFail:
                    return (-1, cmdVar)
            else:
                allDone = 0
        return (allDone, None)
    
    def varCallback(self, *args, **kargs):
        """Check state of script runner and fail or continue if appropriate
        """
        currState, cmdVar = self.state
        if currState < 0:
            self.fail(cmdVar)
        elif currState > 0:
            self._continue(self.retVal)
    
    def cancelWait(self):
        """Call when aborting early.
        """
#       print "_WaitCmdVars.cancelWait"
        self.cleanup()
        for cmdVar in self.cmdVars:
            cmdVar.abort()
    
    def cleanup(self):
        """Called when ending for any reason.
        """
#       print "_WaitCmdVars.cleanup"
        if self.addedCallback:
            for cmdVar in self.cmdVars:
                if not cmdVar.isDone:
                    didRemove = cmdVar.removeCallback(self.varCallback, doRaise=False)
                    if not didRemove:
                        sys.stderr.write("_WaitCmdVar cleanup could not remove callback from %s\n" % (cmdVar,))

    def fail(self, cmdVar):
        """A command var failed.
        """
#       print "_WaitCmdVars.fail(%s)" % (cmdVar,)
        self.scriptRunner._cmdFailCallback(cmdVar)


class _WaitKeyVar(_WaitBase):
    """Wait for one keyword variable, returning the value in scriptRunner.value.
    """
    def __init__(self,
        scriptRunner,
        keyVar,
        ind,
        defVal,
        waitNext,
    ):
        """
        Inputs:
        - scriptRunner: a ScriptRunner instance
        - keyVar    keyword variable
        - ind       index of desired value (None for all values)
        - defVal    value to return if value cannot be determined; if Exception, the script halts
        - waitNext  if True, ignore the current value and wait for the next transition.
        """
        self.keyVar = keyVar
        self.ind = ind
        self.defVal = defVal
        self.waitNext = bool(waitNext)
        self.addedCallback = False
        _WaitBase.__init__(self, scriptRunner)
        
        if self.keyVar.isCurrent and not self.waitNext:
            # no need to wait; value already known
            # schedule a wakeup for asap
            Timer(0.001, self.varCallback)
        elif self.scriptRunner.debug:
            # display message
            argList = ["keyVar=%s" % (keyVar,)]
            if ind != 0:
                argList.append("ind=%s" % (ind,))
            if defVal != Exception:
                argList.append("defVal=%r" % (defVal,))
            if waitNext:
                argList.append("waitNext=%r" % (waitNext,))
            print("waitKeyVar(%s)" % ", ".join(argList))

            # prevent the call from failing by using None instead of Exception
            if self.defVal == Exception:
                self.defVal = None

            Timer(0.001, self.varCallback)
        else:
            # need to wait; set self as a callback
#           print "_WaitKeyVar adding callback"
            self.keyVar.addCallback(self.varCallback, callNow=False)
            self.addedCallback = True
    
    def varCallback(self, keyVar):
        """Set scriptRunner.value to value. If value is invalid,
        use defVal (if specified) else cancel the wait and fail.
        """
#       print "_WaitKeyVar.varCallback(keyVar=%r) % (keyVar,)"
        if self.keyVar.isCurrent:
            self._continue(self.getVal())
        elif self.defVal != Exception:
            self._continue(self.defVal)
        else:
            self.fail("Value of %s invalid" % (self.keyVar,))
    
    def cleanup(self):
        """Called when ending for any reason.
        """
#       print "_WaitKeyVar.cleanup"
        if self.addedCallback:
            self.keyVar.removeCallback(self.varCallback, doRaise=False)
        
    def getVal(self):
        """Return current value[ind] or the list of values if ind=None.
        """
        if self.ind != None:
            return self.keyVar[self.ind]
        else:
            return self.keyVar.valueList


class _WaitThread(_WaitBase):
    def __init__(self, scriptRunner, func, *args, **kargs):
#       print "_WaitThread.__init__(%r, *%r, **%r)" % (func, args, kargs)
        self._pollTimer = Timer()
        _WaitBase.__init__(self, scriptRunner)
        
        if not callable(func):
            raise ValueError("%r is not callable" % func)

        self.queue = Queue.Queue()
        self.func = func

        self.threadObj = threading.Thread(target=self.threadFunc, args=args, kwargs=kargs)
        self.threadObj.setDaemon(True)
        self.threadObj.start()
        self._pollTimer.start(_PollDelaySec, self.checkEnd)
#       print "_WaitThread__init__(%r) done" % self.func
    
    def checkEnd(self):
        if self.threadObj.isAlive():
            self._pollTimer.start(_PollDelaySec, self.checkEnd)
            return
#       print "_WaitThread(%r).checkEnd: thread done" % self.func
        
        retVal = self.queue.get()
#       print "_WaitThread(%r).checkEnd; retVal=%r" % (self.func, retVal)
        self._continue(val=retVal)
        
    def cleanup(self):
#       print "_WaitThread(%r).cleanup" % self.func
        self._pollTimer.cancel()
        self.threadObj = None

    def threadFunc(self, *args, **kargs):
        retVal = self.func(*args, **kargs)
        self.queue.put(retVal)


if __name__ == "__main__":
    from . import cmdkeydispatcher
    import time
    from twisted.internet import reactor
    
    dispatcher = cmdkeydispatcher.CmdKeyVarDispatcher()
    
    scriptList = []

    def initFunc(sr):
        global scriptList
        print("%s init function called" % (sr,))
        scriptList.append(sr)

    def endFunc(sr):
        print("%s end function called" % (sr,))
    
    def script(sr):
        def threadFunc(nSec):
            time.sleep(nSec)
        nSec = 1.0
        sr.showMsg("%s waiting in a thread for %s sec" % (sr, nSec))
        yield sr.waitThread(threadFunc, 1.0)
        
        for val in range(5):
            sr.showMsg("%s value = %s" % (sr, val))
            yield sr.waitMS(1000)
    
    def stateFunc(sr):
        state, reason = sr.fullState
        if reason:
            msgStr = "%s state=%s: %s" % (sr, state, reason)
        else:
            msgStr = "%s state=%s" % (sr, state)
        sr.showMsg(msgStr)
        for sr in scriptList:
            if not sr.isDone:
                return
        reactor.stop()

    sr1 = ScriptRunner(
        runFunc = script,
        name = "Script 1",
        dispatcher = dispatcher,
        initFunc = initFunc,
        endFunc = endFunc,
        stateFunc = stateFunc,
    )
    
    sr2 = ScriptRunner(
        runFunc = script,
        name = "Script 2",
        dispatcher = dispatcher,
        initFunc = initFunc,
        endFunc = endFunc,
        stateFunc = stateFunc,
    )
    
    # start the scripts in a staggared fashion
    sr1.start()
    Timer(1.5, sr1.pause)
    Timer(3.0, sr1.resume)
    Timer(2.5, sr2.start)
    
    reactor.run()
