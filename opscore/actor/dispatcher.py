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
                    Added _replyCmdVar to centralize sending messages to cmdVars
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
2009-03-25 ROwen    Fixed a bug that made KeyVar refresh inefficient (also fixed in RO.KeyDispatcher).

TO DO:
- Clean up use of "connection" once I know what I'll have available
- Fix support for refresh commands, once it is implemented in KeyVar
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
import msgseverity
import keyvar

__all__ = ["KeyVarDispatcher"]

# intervals (in milliseconds) for various background tasks
_RefreshInterval = 1.0 # time interval between variable refresh checks (sec)
_TimeoutInterval = 1.3 # time interval between command timeout checks (sec)
_ShortInterval =   0.01 # short time interval; used to schedule a callback right after pending events (sec)

_CmdNumWrap = 1000 # value at which user command ID numbers wrap

class KeyVarDispatcher(object):
    """Parse replies and sets keyword variables. Also manage commands and their replies.
    """
    def __init__(self,
        name = "KeyVarDispatcher",
        connection = None,
        logFunc = None,
    ):
        """Create a new KeyVarDispatcher
    
        Inputs:
        - name: used as the actor when the dispatcher reports errors
        - connection: an RO.Comm.HubConnection object or similar;
          if omitted, an RO.Comm.HubConnection.NullConnection is used, which is useful for testing.
        - logFunc: a function that logs a message. Argument list must be:
            (msgStr, severity, actor, cmdr)
            where the first argument is positional and the others are by name
            and severity is an RO.Constants.sevX constant
        """
        self.name = name

        self.parser = protoParse.ReplyParser()
        self.reactor = twisted.internet.reactor
        self._isConnected = False

        # dictionary of lists of KeyVars; keys are (actor.lower(), keyName.lower()) tuples;
        # values are lists of KeyVars
        # (having a list of KeyVars allows more than one KeyVar for the same actor keyword)
        self.keyVarListDict = dict()

        # set of actors for which loadActorDictionary has been called
        self.loadedActors = set()
        
        # cmdDict keys are command ID and values are KeyCommands
        self.cmdDict = dict()
        
        # refreshCmdDict contains info about pending refresh commands;
        # it is used to avoid issuing multiple identical commands
        # and to verify that the command successfully refreshes the keyVar.
        # keys are (actor, command string)
        # values are:
        # - keyVar if pending
        # - None if succeeded
        # - False if the command failed
        # Note: if the command succeeds but its keyVar is not refreshed
        # then a warning is printed and the keyVar's refresh command is nulled
        self.refreshCmdDict = {}
        
        # timers for various scheduled callbacks
        self._checkCmdTimer = None
        self._checkRemCmdTimer = None
        self._refreshAllTimer = None
        self._refreshRemTimer = None
        
        if connection:
            self.connection = connection
            self.connection.addReadCallback(self.handleReplyStr)
            self.connection.addStateCallback(self.updConnState)
        else:
            self.connection = RO.Comm.HubConnection.NullConnection()
        self._isConnected = self.connection.isConnected()
        self.userCmdIDGen = RO.Alg.IDGen(1, _CmdNumWrap)
        self.refreshCmdIDGen = RO.Alg.IDGen(_CmdNumWrap + 1, 2 * _CmdNumWrap)
        
        self.setLogFunc(logFunc)        
        
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
        self._replyCmdVar(cmdVar, errReply)

    def addKeyVar(self, keyVar):
        """
        Adds a keyword variable (opscore.actor.keyvar.KeyVar) to the collection.
        
        Inputs:
        - keyVar: the keyword variable (opscore.actor.keyvar.KeyVar)
        """
        dictKey = self._makeDictKey(keyVar.actor, keyVar.name)
        # get list of existing keyVars with the same actor and keyword name,
        # adding an empty list to self.keyVarListDict if none are already present
        keyList = self.keyVarListDict.setdefault(dictKey, [])
        # append new keyVar to the list
        keyList.append(keyVar)

    def checkCmdTimeouts(self):
        """Check all pending commands for timeouts"""
#       print "RO.KeyVarDispatcher.checkCmdTimeouts()"
        
        # cancel pending update, if any
        cancelTimer(self._checkCmdTimer)
        cancelTimer(self._checkRemCmdTimer)
        
        # iterate over a copy of the values
        # so we can modify the dictionary while checking command timeouts
        cmdVarIter = iter(self.cmdDict.values())
        self._checkRemCmdTimeouts(cmdVarIter)

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
#       print "RO.KeyVarDispatcher._checkRemCmdTimeouts(%s)" % cmdVarIter
        try:
            errReply = None
            currTime = time.time()
            for cmdVar in cmdVarIter:
                # if cmd still exits (i.e. has not been deleted for other reasons)
                # check if it has a time limit and has timed out
                if not self.cmdDict.has_key(cmdVar.cmdID):
                    continue
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
                        dataStr = "Timeout; Actor=%r; Cmd=%r" % (cmdVar.actor, cmdVar.cmdStr),
                    )
                    break
            if errReply:
                self._replyCmdVar(cmdVar, errReply)
    
                # schedule myself to run again shortly
                # (thereby giving other time to other events)
                # continuing where I left off
                self._checkRemCmdTimer = self.reactor.callLater(_ShortInterval, self._checkRemCmdTimeouts, cmdVarIter)
        except:
            sys.stderr.write ("RO.KeyVarDispatcher._checkRemCmdTimeouts failed\n")
            traceback.print_exc(file=sys.stderr)

        # finished checking all commands in the current cmdVarIter;
        # schedule a new checkCmdTimeouts at the usual interval
        self._checkCmdTimer = self.reactor.callLater(_TimeoutInterval, self.checkCmdTimeouts)
    
    def dispatch(self, reply):
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
        actor = reply.header.actor.lower()
        cmdr = reply.header.commander
        if actor.startswith("keys."):
            # data is from the hub's keyword cache
            actor = actor[5:]
            isGenuine = False
        else:
            isGenuine = True
        for keyword in reply.keywords:
            keyVarList = self.getKeyVarList(actor, keyword.name)
            for keyVar in keyVarList:
                try:
                    keyVar.set(keyword.values, isGenuine=isGenuine, reply=reply)
                except:
                    traceback.print_exc(file=sys.stderr)

        # if you are the commander for this message,
        # execute the command callback (if any)
        if cmdr == self.connection.cmdr:
            # get the command for this command id, if any
            cmdVar = self.cmdDict.get(cmdID, None)
            if cmdVar != None:
                # send reply but don't log (that's already been done)
                self._replyCmdVar(cmdVar, reply, doLog=False)

    def getKeyVarList(self, actor, keyName):
        """Return the list of KeyVars by this name and actor; return [] if no match.
        
        Do not modify the returned list. It may not be a copy.
        """
        return self.keyVarListDict.get(self._makeDictKey(actor, keyName), [])

    def getKeyVar(self, actor, keyName):
        """Return a keyVar by this name and actor.
        
        If there are multiple matching keyVars returns the first registered;
        to get a different keyVar use getKeyVarList.
        
        Raise LookupError if no such keyword found.
        """
        keyVarList = self.getKeyVarList(actor, keyName)
        if not keyVarList:
            raise LookupError("Could not find a keyVar with actor=%s, keyName=%s" % (actor, keyName))
        return keyVarList[0]
                    
    def handleReplyStr(self, replyStr):
        """Read, parse and dispatch a message from the hub.
        """
        # parse message; if that fails, log it as an error
        try:
            reply = self.parser.parse(replyStr)
        except Exception, e:
            self.logMsg(
                msgStr = "CouldNotParse; Msg=%r; Text=%r" % (replyStr, RO.StringUtil.strFromException(e)),
                severity = RO.Constants.sevError,
            )
            return
        
        # log message
        self.logReply(reply)
        
        # dispatch message
        try:
            self.dispatch(reply)
        except Exception, e:
            sys.stderr.write("Could not dispatch: %r\nwhich was parsed as reply=%r\n" % (replyStr, reply))
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
            self._replyCmdVar(cmdVar, errReply)
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
            fullCmd = "%d %s %s" % (cmdVar.cmdID, cmdVar.actor, cmdVar.cmdStr)
            self.connection.writeLine (fullCmd)
            self.logMsg (
                msgStr = fullCmd,
                actor = cmdVar.actor,
            )
#           print "executing:", fullCmd
        except Exception, e:
            errReply = self.makeReply(
                cmdID = cmdVar.cmdID,
                dataStr = "WriteFailed; Actor=%r; Cmd=%r; Text=%r" % (
                    cmdVar.actor, cmdVar.cmdStr, RO.StringUtil.strFromException(e)),
            )
            self._replyCmdVar(cmdVar, errReply)
        
    def updConnState(self, conn):
        """If connection state changes, update refresh variables.
        """
        wasConnected = self._isConnected
        self._isConnected = conn.isConnected()

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
            sys.stderr.write("Could not log: %r; severity=%r; actor=%r; cmdr=%r\n" % \
                (msgStr, severity, actor, cmdr))
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
            severity = msgseverity.MsgCodeSeverityDict[msgCode]
            self.logMsg(
                msgStr = reply.string,
                severity = severity,
                actor = reply.header.actor,
                cmdr = reply.header.commander,
            )
        except Exception, e:
            sys.stderr.write("Could not log Reply:\n%r\n" % (reply,))
            traceback.print_exc(file=sys.stderr)
        
    def makeReply(self,
        cmdr = None,
        cmdID = 0,
        actor = None,
        msgType = "f",
        dataStr = "",
    ):
        """Generate a Reply object (opscore.protocols.messages.Reply) based on the supplied data.
        
        Useful for reporting internal errors.
        """
#        print "%s.makeReply(cmdr=%s, cmdID=%s, actor=%s, msgType=%s, dataStr=%r)" % \
#            (self.__class__.__name__, cmdr, cmdID, actor, msgType, dataStr)
        if cmdr == None:
            cmdr = self.connection.cmdr
        if actor == None:
            actor = self.name

        headerStr = "%s %d %s %s" % (cmdr, cmdID, actor, msgType)
        try:
            msgStr = " ".join((headerStr, dataStr))
            reply = self.parser.parse(msgStr)
        except Exception, e:
            errStr = "Internal error; could not create message: error: %s" % (RO.StringUtil.strFromException(e),)
            self.logMsg(msgStr = errStr, severity = RO.Constants.sevError)
            sys.stderr.write(errStr + "\n")
            traceback.print_exc(file=sys.stderr)
            # try again with simpler data; give up and raise an exception if that fails
            newDataStr = "Text=%s" % (RO.StringUtil.quoteStr(dataStr))
            newMsgStr = " ".join((headerStr, newDataStr))
            reply = self.parser.parse(newMsgStr)            
        return reply
    
    def refreshAllVar(self, ignoreFailed=False, startOver=False):
        """Examines all keywords, looking for ones that need updating
        and issues the appropriate refresh commands.

        Inputs:
        - ignoreFailed: refresh even if an earlier attempt failed
        - startOver: list all vars as bad and then reload
        """
#       print "RO.KeyVarDispatcher.refreshAllVar(ignoreFailed=%s, startOver=%s)" % (ignoreFailed, startOver)

        # cancel pending update, if any
        cancelTimer(self._refreshAllTimer)
        cancelTimer(self._refreshRemTimer)
    
        if (not self._isConnected) or startOver:
            # clear the refresh command dict and invalidate all keyVars
            # (leave pending refresh commands alone; they will time out)
            self.refreshCmdDict = {}
            for keyVarList in self.keyVarListDict.values():
                for keyVar in keyVarList:
                    keyVar.setNotCurrent()

            # if starting over, schedule a new refreshAllVar ASAP (let pending events run first)
            if self._isConnected:
                self._refreshAllTimer = self.reactor.callLater(_ShortInterval, self.refreshAllVar)

        elif self._isConnected:
            # iterate over a copy of the values
            # so we can modify the dictionary while checking command timeouts
            keyVarListIter = iter(self.keyVarListDict.values())
            self._refreshRemVars(keyVarListIter, ignoreFailed)

    def _refreshRemVars(self, keyVarListIter, ignoreFailed):
        """Helper function for refreshAllVar.
        
        Plow through a keyVarList iterator until a refresh command is found that is wanted, issue it,
        then schedule a call for myself for ASAP (giving other events a chance to execute first).
        
        Inputs:
        - keyVarListIter: iterator over the values of self.keyVarListDict;
            for the first call the iterator should be fresh,
            but as this method calls itself the iterator becomes exhausted
        - ignoreFailed: if True/False then try/skip refresh commands that failed last time

        Once the iterator is exhausted, schedule refreshAllVar to run at the usual interval later.
        """
#       print "RO.KeyVarDispatcher._refreshRemVars(keyVarListIter=%s, ignoreFailed=%s)" % (keyVarListIter, ignoreFailed)
        try:
            if not self._isConnected:
                # schedule parent function asap and bail out
                self._refreshAllTimer = self.reactor.callLater(_ShortInterval, self.refreshAllVar)
                return
                
            # refresh all variables
            for keyVarList in keyVarListIter:
                for keyVar in keyVarList:
                    if not keyVar.isCurrent and keyVar.hasRefreshCmd():
                        # refreshInfo is (actor, cmdStr, timeLim)
                        # and refreshCmdDict keys are (actor, cmdStr)
                        cmdState = self.refreshCmdDict.get(keyVar.getRefreshInfo()[0:2], None)
                        if cmdState == None or (ignoreFailed and cmdState == False):
                            self.refreshOneVar(keyVar)
                            
                            # schedule myself to run again shortly
                            # (thereby giving other time to other events)
                            # continuing where I left off but with a new actor, keyword combo
                            # (since the current one has presumably now been handled)
                            self._refreshRemTimer = self.reactor.callLater(_ShortInterval,
                                self._refreshRemVars, keyVarListIter, ignoreFailed)
                            return
        except:
            sys.stderr.write ("RO.KeyVarDispatcher.refreshAllVar failed:\n")       
            traceback.print_exc(file=sys.stderr)

        # finished checking all commands in the current keyVarListIter;
        # schedule a new refreshAllVar at the usual interval
        self._refreshAllTimer = self.reactor.callLater(_RefreshInterval, self.refreshAllVar)
            
    def refreshOneVar(self, keyVar):
        """Refresh the specified keyVariable.
        """
        if not keyVar.hasRefreshCmd():
            return
#       print "refreshOneVar(%s) with %s" % (keyVar, keyVar.getRefreshInfo())
        actor, cmdStr, timeLim = keyVar.getRefreshInfo()
        cmdVar = keyvar.CmdVar (
            actor = actor,
            cmdStr = cmdStr,
            timeLim = timeLim,
            isRefresh = True,
        )
        self.refreshCmdDict[(actor, cmdStr)] = keyVar
        self.executeCmd(cmdVar)
    
    def removeKeyVar(self, keyVar):
        """Remove the specified keyword variable, returning the KeyVar if removed, else None

        See also "addKeyVar".

        Inputs:
        - keyVar: the keyword variable to remove

        Returns:
        - the removed keyVar, if present, None otherwise.
        """
        dictKey = self._makeDictKey(keyVar.actor, keyVar.name)
        keyVarList = self.keyVarListDict.get(dictKey, [])
        if keyVar in keyVarList:
            keyVarList.remove(keyVar)
            return keyVar
        else:
            return None

    @staticmethod
    def _makeDictKey(actor, keyName):
        """Make a keyVarListDict key out of an actor and keyword name
        """
        return (actor.lower(), keyName.lower())
    
    def _replyCmdVar(self, cmdVar, reply, doLog=True):
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
                sys.stderr.write("KeyVarDispatcher bug: tried to delete cmd %s=%s but it was missing\n" % \
                    (cmdVar.cmdID, cmdVar))

            if cmdVar.isRefresh:
                # refresh command finished; update refresh command dict
                refreshKey = (cmdVar.actor, cmdVar.cmdStr)
                if cmdVar.lastType == ":":
                    # command succeeded;
                    # remove command from refresh command dict
                    # if associated keyVar not updated, complain and delete its refresh command
                    keyVar = self.refreshCmdDict.get(refreshKey)
                    if keyVar == None:
                        return

                    self.refreshCmdDict[refreshKey] = None
                    if not keyVar.isCurrent:
                        keyVar.refreshCmd = None
                        errReply = self.makeReply(
                            cmdID = cmdVar.cmdID,
                            actor = None,
                            msgType = "w",
                            dataStr = "RefreshFailed; Actor=%r; Keyword=%r; Cmd=%r; Text=\"deleting refresh command\"" % \
                                (keyVar.actor, keyVar.keyword, cmdVar.cmdStr),
                        )       
                        self.logReply(errReply)
                else:
                    # command failed, mark it false in the refresh command dict
                    self.refreshCmdDict[refreshKey] = False

    def setLogFunc(self, logFunc=None):
        """Set the log output device, or clears it if None specified.
        
        The function must take the following arguments: (msgStr, severity, actor, cmdr)
        where the first argument is positional and the others are by name
        """
        self.logFunc = logFunc


    def loadActorDictionary(self, actor):
        """Read and process the keys dictionary for an actor.
        
        Registers all the keyVars with this dispatcher
        and returns a dictionary of keyName:keyVar
        
        Does nothing and returns None if the actor was already loaded.
        
        Design note: this is the preferred way to load an actor keys dictionary.
        It is intended to be called by the Model for an actor, which then stores
        the returned keyVarDict as the official set of keyVars for that actor.
        
        This is better than having a Model query the dispatcher for keyVars for an actor,
        because the dispatcher might have duplicates (in which case, which to pick?) or
        extras (such as synthetic keywords) added by miscellaneous bits of code.
        Admittedly that is unlikely when the system is starting up and reading in
        the official dictionaries, but still...the chosen design is safer.
        """
        if actor in self.loadedActors:
            return None
            
        keyVarDict = dict()
        keysDict = protoKeys.KeysDictionary.load(actor)
        for key in keysDict.keys.itervalues():
            keyVar = keyvar.KeyVar(actor, key)
            keyVarDict[key.name.lower()] = keyVar
        for keyVar in keyVarDict.itervalues():
            self.addKeyVar(keyVar)
        self.loadedActors.add(actor)
        return keyVarDict


if __name__ == "__main__":
    print "\nTesting RO.KeyVarDispatcher\n"
    import time
    import opscore.protocols.types as protoTypes
    import twisted.internet.tksupport
    import Tkinter
    root = Tkinter.Tk()
    twisted.internet.tksupport.install(root)
    
    kvd = KeyVarDispatcher()

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
        callCodes = protoMess.ReplyHeader.DoneCodes,
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
        msgType = ":",
        dataStr = dataStr,
    )
    print "\nDispatching message with wrong cmdID; only KeyVar callbacks should called:"
    kvd.dispatch(reply)

    reply = kvd.makeReply(
        cmdID = cmdID,
        actor = "wrongActor",
        msgType = ":",
        dataStr = dataStr,
    )
    print "\nDispatching message with wrong actor; only CmdVar callbacks should be called:"
    kvd.dispatch(reply)

    reply = kvd.makeReply(
        cmdID = cmdID,
        actor = "test",
        msgType = ":",
        dataStr = dataStr,
    )
    print "\nDispatching message correctly; CmdVar done so only KeyVar callbacks should be called:"
    kvd.dispatch(reply)
    
    print "\nTesting keyVar refresh"
    kvd.refreshAllVar()
