#!/usr/bin/env python
"""Sends commands (of type opscore.actor.keyvar.CmdVar) and dispatches replies
to key variables (opscore.actor.keyvar.KeyVar and subclasses).

History:
2002-11-25 ROwen    First version with history.
                    Modified TypeDict to include meaning (in addition to category).
                    Added AllTypes.
2002-12-13 ROwen    Modified to work with the MC.
2003-03-20 ROwen    Added actor to logged commands.
2003-03-26 ROwen    Prevented infinite repeat of failed refresh requests, whether
                    cmd failed or cmd succeeded but did not refresh the keyVar;
                    added ignoreFailed flag to refreshAllVar
                    
2003-05-08 ROwen    Modified to use RO.CnvUtil.
2003-06-09 ROwen    Modified to look up commands purely by command ID, not by actor;
                    this allows us to detect some hub rejections of commands.
2003-06-11 ROwen    Modified to make keyword dispatching case-blind;
                    bug fix in dispatch; refreshKey sometimes referenced before set.
2003-06-18 ROwen    Modified to print a full traceback for unexpected errors.
2003-06-25 ROwen    Modified to handle message data as a dict.
2003-07-10 ROwen    Added makeMsgDict and used it to improve
                    logging and reporting of errors.
2003-07-16 ROwen    Modified to use KeyVar.refreshTimeLim
2003-08-13 ROwen    Moved TypeDict and AllTypes to KeyVariable to remove
                    a circular dependency.
2003-10-10 ROwen    Modified to use new RO.Comm.HubConnection.
2003-12-17 ROwen    Modified KeyVar to support the actor "keys",
                    which is used to refresh values from a cache,
                    to save querying the original actor:
                    - keywords from keys.<actor> are treated as if from <actor>
                    - uses KeyVar.refreshInfo to handle refresh commands.
2004-01-06 ROwen    Modified to use KeyVar.hasRefreshCmd and keyVar.getRefreshInfo
                    instead of keyVar.refreshInfo.
2004-02-05 ROwen    Modified logMsg to make it easier to use; \n is automatically appended
                    and typeCategory can be derived from typeChar.
2004-06-30 ROwen    Added abortCmdByID method to KeyVarDispatcher.
                    Modified for RO.Keyvariable.KeyCommand->CmdVar.
2004-07-23 ROwen    When disconnected, all pending commands time out.
                    Improved variable refresh and command variable timeout handling
                    to better allow other tasks to run: eliminated the use of
                    update_idletasks in favor of scheduling a helper function
                    that works through an iterator and reschedules itself
                    until the iterator is exhausted, then schedules the main task.
                    If a refresh command fails, the message is now printed to the log, not stderr.
                    Added _replyToCmdVar to centralize sending messages to cmdVars
                    and handling completion of commands.
                    If a command ID is already in use, the next ID is assigned;
                    this allows a command to never finish without causing other problems later.
2004-08-13 ROwen    Bug fix: abortCmdByID could report a bug when none existed.
2004-09-08 ROwen    Made NullLogger.addOutput output to stderr instead of discarding the data.
2004-10-12 ROwen    Modified to not keep refreshing keyvars when not connected.
2005-01-05 ROwen    Improved documentation for logMsg.
2005-06-03 ROwen    Bug fix: _isConnected was not getting set properly
                    if connection was omitted. It may also have not been
                    set correctly for real connections in special cases.
                    Bug fix: the test code had a typo; it now works.
2005-06-08 ROwen    Changed KeyVarDispatcher, NullLogger and StdErrLogger to new style classes.
2005-06-16 ROwen    Modified logMsg to take severity instead of typeChar.
2006-05-01 ROwen    Bug fix: if a message could not be parsed, logging the error failed
                    (due to logging in a way that involved parsing the message again).
2006-10-25 ROwen    Overhauled logging:
                    - Replaced logger argument with logFunc.
                    - Replaced setLogger method with setLogFunc
                    - Modified logMsg method to support the new log function:
                      - Removed typeCategory and msgID arguments.
                      - Added actor and cmdr arguments.
                    Modified to log commands using the command target as the actor, not TUI.
2008-04-29 ROwen    Fixed reporting of exceptions that contain unicode arguments.
2009-01-06 ROwen    Improved some doc strings.
2009-03-24 ROwen    Moved to opscore and modified to use opscore.
2009-03-25 ROwen    Fixed a bug that made KeyVar refresh inefficient (also fixed in opscore.actor.CmdKeyVarDispatcher).
2009-04-03 ROwen    Split out keyvar functionality into a very simple base class.
2009-07-18 ROwen    Overhauled keyVar refresh to be more efficient and to run each refresh command only once.
2009-07-20 ROwen    Modified to not log if logFunc = None; added a convenience logging function.
2009-07-21 ROwen    Added support for including commander info in command strings.
2009-07-23 ROwen    Added delayCallbacks argument and sendAllKeyVarCallbacks method.
2009-08-24 ROwen    Test for valid name at creation time.
                    Improved error reporting in makeReply.
                    If timing out a command fails, don't try to time it out again.
2009-08-25 ROwen    Bug fix: failed to dispatch replies to cmdVars with forUserCmd set, because it
                    ignored replies with a prefix on the cmdr.
                    Added replyIsMine method.
"""
import sys
import time
import traceback

import twisted.internet.reactor
import RO.Alg
import RO.Comm.HubConnection
import RO.Constants
import RO.StringUtil

from opscore.utility.twisted import cancelTimer
import opscore.protocols.keys as protoKeys
import opscore.protocols.parser as protoParse
import opscore.protocols.messages as protoMess
import keydispatcher
import keyvar

__all__ = ["logToStdOut", "CmdKeyVarDispatcher"]

# intervals (in milliseconds) for various background tasks
_RefreshInterval = 1.0 # time interval between variable refresh checks (sec)
_TimeoutInterval = 1.3 # time interval between command timeout checks (sec)
_ShortInterval =   0.01 # short time interval; used to schedule a callback right after pending events (sec)

_CmdNumWrap = 1000 # value at which user command ID numbers wrap

_RefreshTimeLim = 20 # time limit for refresh commands (sec)


def logToStdOut(msgStr, severity, actor, cmdr):
    print msgStr


class CmdKeyVarDispatcher(keydispatcher.KeyVarDispatcher):
    """Parse replies and sets KeyVars. Also manage CmdVars and their replies.
    """
    def __init__(self,
        name = "CmdKeyVarDispatcher",
        connection = None,
        logFunc = None,
        includeName = True,
        delayCallbacks = False,
    ):
        """Create a new CmdKeyVarDispatcher
    
        Inputs:
        - name: dispatcher name; must be a valid actor name (_ is OK; avoid other punctuation and whitespace).
            Used when the dispatcher reports errors.
            If includeName is True, then sent as a prefix to all commands sent to the hub.
        - connection: an RO.Comm.HubConnection object or similar;
          if omitted, an RO.Comm.HubConnection.NullConnection is used, which is useful for testing.
        - logFunc: a function that logs a message. Argument list must be:
            (msgStr, severity, actor, cmdr)
            where the first argument is positional and the others are by name
            and severity is an RO.Constants.sevX constant
            If None then nothing is logged.
        - includeName: if True then self.name is prepended to all commands sent to the hub,
            or if cmdVar.forUserCmd is present, then the prefix is <cmdVar.forUserCmd.cmdr>.<self.name>.
        - delayCallbacks: if True then upon initial connection no KeyVar callbacks are made
            until all refresh commands have completed (at which point callbacks are made
            for each keyVar that has been set). Thus the set of keyVars will be maximally
            self-consistent, but it may take awhile after connecting before callbacks begin.

        Raises ValueError if name cannot be used as an actor name
        """
        keydispatcher.KeyVarDispatcher.__init__(self)
        
        self.name = name
        self.includeName = bool(includeName)
        self.delayCallbacks = bool(delayCallbacks)
        
        self.parser = protoParse.ReplyParser()
        self.reactor = twisted.internet.reactor
        self._isConnected = False

        # cmdDict keys are command ID and values are KeyCommands
        self.cmdDict = dict()
        
        # refreshCmdDict contains information about keyVar refresh commands:
        # key is: actor, refresh command, e.g. as returned by keyVar.refreshInfo
        # refresh command: set of keyVars that use this command
        self.refreshCmdDict = {}

        # list of refresh commands that have been executed; used to support delayCallbacks
        self._runningRefreshCmdSet = set()
        self._allRefreshCmdsSent = False
        self._enableCallbacks = not self.delayCallbacks
        
        # timers for various scheduled callbacks
        self._checkCmdTimer = None
        self._checkRemCmdTimer = None
        self._refreshAllTimer = None
        self._refreshNextTimer = None
        
        if connection:
            def readCallback(sock, data):
                self.dispatchReplyStr(data)
            self.connection = connection
            self.connection.addReadCallback(readCallback)
            self.connection.addStateCallback(self.updConnState)
        else:
            self.connection = RO.Comm.HubConnection.NullConnection()
        self._isConnected = self.connection.isConnected()
        self.userCmdIDGen = RO.Alg.IDGen(1, _CmdNumWrap)
        self.refreshCmdIDGen = RO.Alg.IDGen(_CmdNumWrap + 1, 2 * _CmdNumWrap)
        
        self.setLogFunc(logFunc)        
        
        try:
            self.makeReply(dataStr="TestName")
        except Exception, e:
            raise ValueError("Invalid name=%s cannot be parsed as an actor name; error: %s" % \
                (name, RO.StringUtil.strFromException(e)))
        
        # start background tasks (refresh variables and check command timeout)
        self.refreshAllVar()
        self.checkCmdTimeouts()
    
    def abortCmdByID(self, cmdID):
        """Abort the command with the specified ID.
        
        Issue the command specified by cmdVar.abortCmdStr, if present.
        Report the command as failed.
        
        Has no effect if the command was never dispatched (cmdID == None)
        or has already finished.
        """
        if cmdID == None:
            return

        cmdVar = self.cmdDict.get(cmdID)
        if not cmdVar:
            return

        # check isDone
        if cmdVar.isDone:
            return
        
        # if relevant, issue abort command, with no callbacks
        if cmdVar.abortCmdStr and self._isConnected:
            abortCmd = keyvar.CmdVar(
                cmdStr = cmdVar.abortCmdStr,
                actor = cmdVar.actor,
            )
            self.executeCmd(abortCmd)
            
        # report command as aborted
        errReply = self.makeReply (
            cmdID = cmdVar.cmdID,
            dataStr = "Aborted; Actor=%r; Cmd=%r" % (cmdVar.actor, cmdVar.cmdStr),
        )
        self._replyToCmdVar(cmdVar, errReply)

    def addKeyVar(self, keyVar):
        """Add a keyword variable (opscore.actor.keyvar.KeyVar) to the collection.
        
        Inputs:
        - keyVar: the keyword variable (opscore.actor.keyvar.KeyVar)
        """
#        print "%s.addKeyVar(%s); hasRefreshCmd=%s; refreshInfo=%s" % (self.__class__.__name__, keyVar, keyVar.hasRefreshCmd, keyVar.refreshInfo)
        keydispatcher.KeyVarDispatcher.addKeyVar(self, keyVar)
        if keyVar.hasRefreshCmd:
            refreshInfo = keyVar.refreshInfo
            keyVarSet = self.refreshCmdDict.get(refreshInfo)
            if keyVarSet:
                keyVarSet.add(keyVar)
            else:
                self.refreshCmdDict[refreshInfo] = set((keyVar,))
            if self._isConnected:
                cancelTimer(self._refreshAllTimer)
                self._refreshAllTimer = self.reactor.callLater(_ShortInterval, self.refreshAllVar)

    def checkCmdTimeouts(self):
        """Check all pending commands for timeouts"""
#       print "opscore.actor.CmdKeyVarDispatcher.checkCmdTimeouts()"
        
        # cancel pending update, if any
        cancelTimer(self._checkCmdTimer)
        cancelTimer(self._checkRemCmdTimer)
        
        # iterate over a copy of the values
        # so we can modify the dictionary while checking command timeouts
        cmdVarIter = iter(self.cmdDict.values())
        self._checkRemCmdTimeouts(cmdVarIter)
    
    def dispatchReply(self, reply):
        """Set KeyVars based on the supplied Reply
        
        reply is a parsed Reply object (opscore.protocols.messages.Reply) whose fields include:
         - header.program: name of the program that triggered the message (string)
         - header.commandId: command ID that triggered the message (int) 
         - header.actor: the actor that generated the message (string)
         - header.code: the message type code (opscore.protocols.types.Enum)
         - string: the original unparsed message (string)
         - keywords: an ordered dictionary of message keywords (opscore.protocols.messages.Keywords)        
        Refer to https://trac.sdss3.org/wiki/Ops/Protocols for details.
        """
        # handle KeyVars
        keydispatcher.KeyVarDispatcher.dispatchReply(self, reply, doCallbacks=self._enableCallbacks)

        # if you are the commander for this message,
        # execute the command callback (if any)
        if self.replyIsMine(reply):
            # get the command for this command id, if any
            cmdVar = self.cmdDict.get(reply.header.commandId, None)
            if cmdVar != None:
                # send reply but don't log (that's already been done)
                self._replyToCmdVar(cmdVar, reply, doLog=False)
                    
    def dispatchReplyStr(self, replyStr):
        """Read, parse and dispatch a message from the hub.
        """
#        print "%s.dispatchReplyStr(%r)" % (self.__class__.__name__, replyStr)
        # parse message; if that fails, log it as an error
        try:
            reply = self.parser.parse(replyStr)
        except Exception, e:
            self.logMsg(
                msgStr = "CouldNotParse; Reply=%r; Text=%r" % (replyStr, RO.StringUtil.strFromException(e)),
                severity = RO.Constants.sevError,
            )
            return
        
        # log message
        self.logReply(reply)
        
        # dispatch message
        try:
            self.dispatchReply(reply)
        except Exception, e:
            sys.stderr.write("Could not dispatch replyStr=%r\n    which was parsed as reply=%r\n" % \
                (replyStr, reply))
            traceback.print_exc(file=sys.stderr)
                
    def executeCmd(self, cmdVar):
        """Execute a command (of type opscore.actor.keyvar.CmdVar).
        
        Performs the following tasks:
        - Sets the command ID number
        - Sets the start time
        - Puts the command on the keyword dispatcher queue
        - Sends the command to the server

        Inputs:
        - cmdVar: the command, of class opscore.actor.keyvar.CmdVar
            
        Note:
        - Always increments cmdID because every command must have a unique command ID
          (even commands that go to different actors); this simplifies the
          dispatcher code and also makes the hub's life easier
          (since it can report certain kinds of failures using actor=hub).
        """
        if not self._isConnected:
            errReply = self.makeReply(
                dataStr = "Failed; Actor=%r; Cmd=%r; Text=\"not connected\"" % (cmdVar.actor, cmdVar.cmdStr),
            )
            self._replyToCmdVar(cmdVar, errReply)
            return
        
        while True:
            if cmdVar.isRefresh:
                cmdID = self.refreshCmdIDGen.next()
            else:
                cmdID = self.userCmdIDGen.next()
            if not self.cmdDict.has_key(cmdID):
                break
        self.cmdDict[cmdID] = cmdVar
        cmdVar._setStartInfo(self, cmdID)
    
        try:
            if self.includeName:
                if cmdVar.forUserCmd:
                    namePrefix = "%s.%s " % (cmdVar.forUserCmd.cmdr, self.name)
                else:
                    namePrefix = "%s " % (self.name)
            else:
                namePrefix = ""
            fullCmdStr = "%s%d %s %s" % (namePrefix, cmdVar.cmdID, cmdVar.actor, cmdVar.cmdStr)
            self.connection.writeLine (fullCmdStr)
            self.logMsg (
                msgStr = fullCmdStr,
                actor = cmdVar.actor,
            )
#             print "executing:", fullCmdStr
        except Exception, e:
            errReply = self.makeReply(
                cmdID = cmdVar.cmdID,
                dataStr = "WriteFailed; Actor=%r; Cmd=%r; Text=%r" % (
                    cmdVar.actor, cmdVar.cmdStr, RO.StringUtil.strFromException(e)),
            )
            self._replyToCmdVar(cmdVar, errReply)

    def replyIsMine(self, reply):
        """Return True if I am the commander for this message.
        """
        return reply.header.cmdrName.endswith(self.connection.cmdr) \
            and reply.header.cmdrName[-len(self.connection.cmdr) - 1: -len(self.connection.cmdr)] in ("", ".")

    def updConnState(self, conn):
        """If connection state changes, update refresh variables.
        """
        wasConnected = self._isConnected
        self._isConnected = conn.isConnected()
#         print "updConnState; wasConnected=%s, isConnected=%s" % (wasConnected, self._isConnected)

        if wasConnected != self._isConnected:
            self.reactor.callLater(_ShortInterval, self.refreshAllVar)
        
    def logMsg(self,
        msgStr,
        severity = RO.Constants.sevNormal,
        actor = "TUI",
        cmdr = None,
    ):
        """Writes a message to the log.
        On error, prints message to stderr and returns normally.
        
        Inputs:
        - msgStr: message to display; a final \n is appended
        - severity: message severity (an RO.Constants.sevX constant)
        - actor: name of actor
        - cmdr: commander; defaults to self
        """
        if not self.logFunc:
            return

        try:
            self.logFunc(
                msgStr,
                severity = severity,
                actor = actor,
                cmdr = cmdr,
            )
        except Exception, e:
            sys.stderr.write("Could not log msgStr=%r; severity=%r; actor=%r; cmdr=%r\n    error: %s\n" % \
                (msgStr, severity, actor, cmdr, RO.StringUtil.strFromException(e)))
            traceback.print_exc(file=sys.stderr)
    
    def logReply(self, reply):
        """Log a reply (an opscore.protocols.messages.Reply)

        reply is a parsed Reply object (opscore.protocols.messages.Reply) whose fields include:
         - header.program: name of the program that triggered the message (string)
         - header.commandId: command ID that triggered the message (int) 
         - header.actor: the actor that generated the message (string)
         - header.code: the message type code (opscore.protocols.types.Enum)
         - string: the original unparsed message (string)
         - keywords: an ordered dictionary of message keywords (opscore.protocols.messages.Keywords)        
        Refer to https://trac.sdss3.org/wiki/Ops/Protocols for details.
        """
        try:
            msgCode = reply.header.code
            severity = keyvar.MsgCodeSeverity[msgCode]
            self.logMsg(
                msgStr = reply.string,
                severity = severity,
                actor = reply.header.actor,
                cmdr = reply.header.cmdrName,
            )
        except Exception, e:
            sys.stderr.write("Could not log reply=%r\n    error=%s\n" % \
                (reply, RO.StringUtil.strFromException(e)))
            traceback.print_exc(file=sys.stderr)
        
    def makeReply(self,
        cmdr = None,
        cmdID = 0,
        actor = None,
        msgCode = "F",
        dataStr = "",
    ):
        """Generate a Reply object (opscore.protocols.messages.Reply) based on the supplied data.
        
        Useful for reporting internal errors.
        """
#        print "%s.makeReply(cmdr=%s, cmdID=%s, actor=%s, msgCode=%s, dataStr=%r)" % \
#            (self.__class__.__name__, cmdr, cmdID, actor, msgCode, dataStr)
        msgStr = None
        try:
            if cmdr == None:
                cmdr = self.connection.cmdr or "me.me"
            if actor == None:
                actor = self.name
    
            headerStr = "%s %d %s %s" % (cmdr, cmdID, actor, msgCode)
            msgStr = " ".join((headerStr, dataStr))
            reply = self.parser.parse(msgStr)
        except Exception:
            sys.stderr.write("%s.makeReply could not make reply from msgStr=%r; will try again with simplified msgStr\n" % \
                (self.__class__.__name__, msgStr))
            traceback.print_exc(file=sys.stderr)
            # try again with simpler data; give up and raise an exception if that fails
            newMsgStr = None
            try:
                newDataStr = "Text=%s" % (RO.StringUtil.quoteStr(dataStr))
                newMsgStr = " ".join((headerStr, newDataStr))
                reply = self.parser.parse(newMsgStr)            
            except Exception:
                sys.stderr.write("%s.makeReply could not make reply from simplified msgStr=%r; giving up\n" % \
                    (self.__class__.__name__, newMsgStr,))
                traceback.print_exc(file=sys.stderr)
                logMsgStr = "Internal error; could not create message from msgStr=%r; see log for details" % (msgStr,)
                self.logMsg(msgStr = logMsgStr, severity = RO.Constants.sevError)
                raise
            else:
                sys.stderr.write("%s.makeReply succeeded with simplified msgStr=%r\n" % (self.__class__.__name__, newMsgStr,))
        return reply

    def refreshAllVar(self):
        """Issue all keyVar refresh commands.
        """
#         print "%s.refreshAllVar(); refeshCmdDict=%s" % (self.__class__.__name__, self.refreshCmdDict)

        # cancel pending update, if any
        cancelTimer(self._refreshAllTimer)
        cancelTimer(self._refreshNextTimer)
        self._enableCallbacks = not self.delayCallbacks
        self._runningRefreshCmdSet = set()
        self._allRefreshCmdsSent = False
        
        if not self._isConnected:
            for keyVarList in self.keyVarListDict.values():
                for keyVar in keyVarList:
                    keyVar.setNotCurrent()
            return
    
        self._sendNextRefreshCmd()

    def removeKeyVar(self, keyVar):
        """Remove the specified keyword variable, returning the KeyVar if removed, else None

        See also addKeyVar.

        Inputs:
        - keyVar: the keyword variable to remove

        Returns:
        - the removed keyVar, if present, None otherwise.
        """
        keyVar = keydispatcher.KeyVarDispatcher.removeKeyVar(self, keyVar)

        keyVarSet = self.refreshCmdDict.get(keyVar.refreshInfo)
        if keyVarSet and keyVar in keyVarSet:
            keyVarSet.remove(keyVar)
            if not keyVarSet:
                # that was the only keyVar using this refresh command
                del(self.refreshCmdDict[keyVar.refreshInfo])
        return keyVar

    def sendAllKeyVarCallbacks(self, includeNotCurrent=True):
        """Send all keyVar callbacks.
        
        Inputs:
        - includeNotCurrent: issue callbacks for keyVars that are not current?
        """
        keyVarListIter = self.keyVarListDict.itervalues()
        self._nextKeyVarCallback(keyVarListIter, includeNotCurrent=False)

    def setLogFunc(self, logFunc=None):
        """Set the log output device, or clears it if None specified.
        
        The function must take the following arguments: (msgStr, severity, actor, cmdr)
        where the first argument is positional and the others are by name
        """
        self.logFunc = logFunc

    def _checkRemCmdTimeouts(self, cmdVarIter):
        """Helper function for checkCmdTimeouts.
        Check the remaining command variables in cmdVarIter.
        If a timeout is found, time out that one command
        and schedule myself to run again shortly
        (thereby giving other events a chance to run).

        Once the iterator is exhausted, schedule
        my parent function checkCmdTimeouts to run
        at the usual interval later.
        """
#       print "opscore.actor.CmdKeyVarDispatcher._checkRemCmdTimeouts(%s)" % cmdVarIter
        try:
            for cmdVar in cmdVarIter:
                errReply = None
                currTime = time.time()
                # if cmd still exits (i.e. has not been deleted for other reasons)
                # check if it has a time limit and has timed out
                if cmdVar.cmdID not in self.cmdDict:
                    continue
                try:
                    if not self._isConnected:
                        errReply = self.makeReply (
                            cmdID = cmdVar.cmdID,
                            dataStr = "Aborted; Actor=%r; Cmd=%r; Text=\"disconnected\"" % (cmdVar.actor, cmdVar.cmdStr),
                        )
                        # no connection, so cannot send abort command
                        cmdVar.abortCmdStr = ""
                        break
                    elif cmdVar.maxEndTime and (cmdVar.maxEndTime < currTime):
                        # time out this command
                        errReply = self.makeReply (
                            cmdID = cmdVar.cmdID,
                            dataStr = "Timeout; Actor=%r; Cmd=%s" % (cmdVar.actor, RO.StringUtil.quoteStr(cmdVar.cmdStr)),
                        )
                        break
                    if errReply:
                        self._replyToCmdVar(cmdVar, errReply)
            
                        # schedule myself to run again shortly
                        # (thereby giving other time to other events)
                        # continuing where I left off
                        self._checkRemCmdTimer = self.reactor.callLater(_ShortInterval, self._checkRemCmdTimeouts, cmdVarIter)
                except Exception:
                    sys.stderr.write("%s._checkRemCmdTimeouts failed to timeout command %s\n" % \
                        (self.__class__.__name__, cmdVar))
                    traceback.print_exc(file=sys.stderr)
                    cmdVar.maxEndTime = None
        except Exception:
            # this is very, very unlikely
            sys.stderr.write("%s._checkRemCmdTimeouts failed\n" % (self.__class__.__name__,))
            traceback.print_exc(file=sys.stderr)

        # finished checking all commands in the current cmdVarIter;
        # schedule a new checkCmdTimeouts at the usual interval
        self._checkCmdTimer = self.reactor.callLater(_TimeoutInterval, self.checkCmdTimeouts)

    @staticmethod
    def _makeDictKey(actor, keyName):
        """Make a keyVarListDict key out of an actor and keyword name
        """
        return (actor.lower(), keyName.lower())

    def _nextKeyVarCallback(self, keyVarListIter, includeNotCurrent=True):
        """Issue next keyVar callback
        
        Input:
        - keyVarListIter: iterator over values in self.keyVarListDict
        """
        try:
            keyVarList = keyVarListIter.next()
        except StopIteration:
            return
        for keyVar in keyVarList:
            if includeNotCurrent or keyVar.isCurrent:
                keyVar.doCallbacks()
        self.reactor.callLater(0.001, self._nextKeyVarCallback, keyVarListIter, includeNotCurrent)

    def _refreshCmdCallback(self, refreshCmd):
        """Refresh command callback; complain if command failed or some keyVars not updated
        """
        if not refreshCmd.isDone:
            return
        try:
            self._runningRefreshCmdSet.remove(refreshCmd)
        except Exception:
            sys.stderr.write("could not find refresh command %s to remove it\n" % (refreshCmd,))
        refreshInfo = (refreshCmd.actor, refreshCmd.cmdStr)
        keyVarSet = self.refreshCmdDict.get(refreshInfo, set())
        if refreshCmd.didFail:
            keyVarNamesStr = ", ".join(sorted([kv.name for kv in keyVarSet]))
            errMsg = "Refresh command %s %s failed; keyVars not refreshed: %s" % \
                (refreshCmd.actor, refreshCmd.cmdStr, keyVarNamesStr)
            self.logMsg(errMsg, severity=RO.Constants.sevWarning)
        elif keyVarSet:
            aKeyVar = iter(keyVarSet).next()
            actor = aKeyVar.actor
            missingKeyVarNamesStr = ", ".join(sorted([kv.name for kv in keyVarSet if not kv.isCurrent]))
            if missingKeyVarNamesStr:
                errMsg = "No refresh data for %s keyVars: %s" % (actor, missingKeyVarNamesStr)
                self.logMsg(errMsg, severity=RO.Constants.sevWarning)
        else:
            # all of the keyVars were removed or there is a bug
            errMsg = "Warning: refresh command %s %s finished but no keyVars found\n" % refreshInfo
            self.logMsg(errMsg, severity=RO.Constants.sevWarning)

        # handle delayCallbacks:
        if not self.delayCallbacks or not self._allRefreshCmdsSent or self._runningRefreshCmdSet:
            return
#         print "using delayCallbacks and the last refresh command has finished; refresh all variables"
        self._enableCallbacks = True
        self.sendAllKeyVarCallbacks(includeNotCurrent=False)
    
    def _replyToCmdVar(self, cmdVar, reply, doLog=True):
        """Send a message to a command variable and optionally log it.

        If the command is done, delete it from the command dict.
        If the command is a refresh command and is done,
        update the refresh command dict accordingly.
        
        Inputs:
        - cmdVar    command variable (opscore.actor.keyvar.CmdVar)
        - reply     Reply object (opscore.protocols.messages.Reply) to send
        """
        if doLog:
            self.logReply(reply)
        cmdVar.handleReply(reply)
        if cmdVar.isDone and cmdVar.cmdID != None:
            try:
                del (self.cmdDict[cmdVar.cmdID])
            except KeyError:
                sys.stderr.write("CmdKeyVarDispatcher bug: tried to delete cmd %s=%s but it was missing\n" % \
                    (cmdVar.cmdID, cmdVar))

    def _sendNextRefreshCmd(self, refreshCmdItemIter=None):
        """Helper function for refreshAllVar.
        
        Plow through a keyVarList iterator until a refresh command is found that is wanted, issue it,
        then schedule a call for myself for ASAP (giving other events a chance to execute first).
        
        Inputs:
        - refreshCmdItemIter: iterator over items in refreshCmdDict;
          if None then set to self.refreshCmdDict.iteritems()
        """
#         print "%s._sendNextRefreshCmd(%s)" % (self.__class__.__name__, refreshCmdItemIter)
        if not self._isConnected:
            # schedule parent function asap and bail out
            self._refreshAllTimer = self.reactor.callLater(_ShortInterval, self.refreshAllVar)
            return

        if refreshCmdItemIter == None:
            refreshCmdItemIter = self.refreshCmdDict.iteritems()

        try:
            refreshCmdInfo, keyVarSet = refreshCmdItemIter.next()
        except StopIteration:
            self._allRefreshCmdsSent = True
            return
        actor, cmdStr = refreshCmdInfo
        try:
            cmdVar = keyvar.CmdVar (
                actor = actor,
                cmdStr = cmdStr,
                timeLim = _RefreshTimeLim,
                callFunc = self._refreshCmdCallback,
                isRefresh = True,
            )
            self._runningRefreshCmdSet.add(cmdVar)
            self.executeCmd(cmdVar)
        except:
            sys.stderr.write("%s._sendNextRefreshCmd: refresh command %s failed:\n" % (self.__class__.__name__, cmdVar,))
            traceback.print_exc(file=sys.stderr)
        self._refreshNextTimer = self.reactor.callLater(_ShortInterval, self._sendNextRefreshCmd, refreshCmdItemIter)


if __name__ == "__main__":
    print "\nTesting opscore.actor.CmdKeyVarDispatcher\n"
    import time
    import opscore.protocols.types as protoTypes
    import twisted.internet.tksupport
    import Tkinter
    root = Tkinter.Tk()
    twisted.internet.tksupport.install(root)
    
    kvd = CmdKeyVarDispatcher()

    def showVal(keyVar):
        print "keyVar %s.%s = %r, isCurrent = %s" % (keyVar.actor, keyVar.name, keyVar.valueList, keyVar.isCurrent)

    # scalars
    keyList = (
        protoKeys.Key("StringKey", protoTypes.String()),
        protoKeys.Key("IntKey", protoTypes.Int()),
        protoKeys.Key("FloatKey", protoTypes.Float()),
        protoKeys.Key("BooleanKey", protoTypes.Bool("F", "T")),
        protoKeys.Key("KeyList", protoTypes.String(), protoTypes.Int()),
    )
    keyVarList = [keyvar.KeyVar("test", key) for key in keyList]
    for keyVar in keyVarList:
        keyVar.addCallback(showVal)
        kvd.addKeyVar(keyVar)
    
    # command callback
    def cmdCall(cmdVar):
        print "command callback for actor=%s, cmdID=%d, cmdStr=%r, isDone=%s" % \
            (cmdVar.actor, cmdVar.cmdID, cmdVar.cmdStr, cmdVar.isDone)
    
    # command
    cmdVar = keyvar.CmdVar(
        cmdStr = "THIS IS A SAMPLE COMMAND",
        actor="test",
        callFunc=cmdCall,
        callCodes = keyvar.DoneCodes,
    )
    kvd.executeCmd(cmdVar)
    cmdID = cmdVar.cmdID

    dataList = [
        "StringKey=hello",
        "IntKey=1",
        "FloatKey=1.23456789",
        "BooleanKey=T",
        "KeyList=three, 3",
        "Coord2Key=45.0, 0.1, 32.1, -0.1, %s" % (time.time(),),
    ]
    dataStr = "; ".join(dataList)

    reply = kvd.makeReply(
        cmdr = "myprog.me",
        cmdID = cmdID - 1,
        actor = "test",
        msgCode = ":",
        dataStr = dataStr,
    )
    print "\nDispatching message with wrong cmdID; only KeyVar callbacks should called:"
    kvd.dispatchReply(reply)

    reply = kvd.makeReply(
        cmdID = cmdID,
        actor = "wrongActor",
        msgCode = ":",
        dataStr = dataStr,
    )
    print "\nDispatching message with wrong actor; only CmdVar callbacks should be called:"
    kvd.dispatchReply(reply)

    reply = kvd.makeReply(
        cmdID = cmdID,
        actor = "test",
        msgCode = ":",
        dataStr = dataStr,
    )
    print "\nDispatching message correctly; CmdVar done so only KeyVar callbacks should be called:"
    kvd.dispatchReply(reply)
    
    print "\nTesting keyVar refresh"
    kvd.refreshAllVar()
