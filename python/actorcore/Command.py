""" Command.py -- essentials of hub commands.

    When commands come in from the hub or some other source, a Command
    instance is created to shepherd it though to completion. A
    Command knows what the command is and where it came from, and provides
    wrappers for responding to the sender.
    
"""
__all__ = ['Command']

import pdb

import logging
from opscore.utility.qstr import qstr

cmdLogger = logging.getLogger('cmds')

class Command(object):
    def __init__(self, source, cmdr, cid, mid, rawCmd, 
                 debug=0, immortal=False):
        """ Create a fully defined command:

        source  - The input object which gets any responses.
        cmdr    - the name of the commander.
        mid 	- Message ID: should uniquely identify the command.
        cid 	- Connection ID: identifies this connection.
        rawCmd	- The unparsed text of the command.

        immortal ? If set, the command cannot be finished or failed.

        """
        
        self.source = source
        self.cmdr = cmdr
        self.cid = cid
        self.mid = mid
        self.rawCmd = rawCmd
        self.cmd = None
        
        self.alive = 1
        self.immortal = immortal
        self.debug = debug
 
        cmdLogger.debug("New Command: %s" % (self))
        
    def __str__(self):
        if self.cmd:
            cmdDescr = 'cmd=%s' % (self.cmd)
        else:
            cmdDescr = 'rawCmd=%s' % (self.rawCmd)
            
        return "Command(source=%08x cmdr=%s cid=%s mid=%s %s)" % \
               (id(self.source), self.cmdr, self.cid, self.mid, cmdDescr)

    @property
    def program(self):
        """ Return the program name component of the cmdr name. """

        try:
            parts = self.cmdr.split('.')
            return parts[0]
        except:
            cmdLogger.critical("failed to get programName from cmdrName %s" % (self.cmdr))
            return ''

    @property
    def username(self):
        """ Return the username component of the cmdr name. """

        try:
            parts = self.cmdr.split('.')
            return parts[1]
        except:
            cmdLogger.critical("failed to get username from cmdrName %s" % (self.cmdr))
            return ''
        
    def isAlive(self):
        """ Is this command still valid (i.e. no fail or finish sent)? """

        return self.alive
    
    def respond(self, response):
        """ Return intermediate response. """

        self.__respond('i', response)

    def inform(self, response):
        """ Return intermediate response. """

        self.__respond('i', response)

    def diag(self, response):
        """ Return diagnostic output. """

        self.__respond('d', response)
        
    def warn(self, response):
        """ Return warning. """

        self.__respond('w', response)
        
    def error(self, response):
        """ Return intermediate error response. """

        self.__respond('e', response)
        
    def finish(self, response=None):
        """ Return successful command finish. """

        if response == None:
            response = ''
            
        if self.immortal:
            self.__respond('i', response)
        else:
            self.__respond(':', response)
            self.alive = 0
        
    def fail(self, response):
        """ Return failure. """

        if self.immortal:
            self.__respond('w', response)
        else:
            self.__respond('f', response)
            self.alive = 0
        
    def sendResponse(self, flag, response):
        """ Return a response with a specific flag. """

        self.__respond(flag, response)
        
    def __respond(self, flag, response):
        """ Actually send the response to the appropriate source. If the command has already
            been finished, try broadcasting a complaint. It is a bit unclear what to do about the
            original response; I'm just passing it along to bother others. """

        if not self.alive:
            self.source.sendResponse(self, 'w', 
                                     'text="this command has already been finished!!!! (%s %s): %s"' % \
                                         (self.cmdr, self.mid, self.rawCmd))
        self.source.sendResponse(self, flag, response)
        # self.actor.bcast.warn('text="sent a response to an already finished command: %s"' % (self))
        
    def coverArgs(self, requiredArgs, optionalArgs=None, ignoreFirst=None):
        """ getopt, sort of.

        Args:
           requiredArgs     - list of words which must be matched.
           optionalArgs     - list of optional words to match
           ignoreFirst      - if set, skip the first cmd word.

        Returns:
           - a dictionary of requiredArgs matches
           - a list of the requiredArgs words which were NOT matched.
           - a dictionary of optionalArgs matches
           - a list of command args not covered by requiredArgs or optionalArgs

        Notes:
           Does not return list of unmatched optionalArgs.
           Pretty much ignores argument order, for better or worse.
        
        command = 'jump height=14 over=cow before=6pm backwards 3 5 5'
        coverArgs(('height', 'backwards', 'withPole'), ('before', 'after')) ->
            {'height':14, 'backwards':None},
            ('withPole'),
            {'before':'6pm'},
            ('over=cow', '3', '5', '5')

        """

        if self.debug > 4:
            CPL.log("MCCommand.coverArgs",
                    "requiredArgs=%r optionalArgs=%r ignoreFirst=%r argDict=%r" \
                    % (requiredArgs, optionalArgs, ignoreFirst, self.argDict))

        # Start with a copy of the command args, which we consume as we copy to
        # the matched_args dict.
        #
        # Match requireArgs before optionalArgs
        #
        if requiredArgs:
            requiredArgs = list(requiredArgs)
        else:
            requiredArgs = []
        if optionalArgs:
            optionalArgs = list(optionalArgs)
        else:
            optionalArgs = []
        
        requiredMatches = {}
        optionalMatches = {}
        leftovers = []

        # Walk down the argument list and categorize the arguments.
        #
        if ignoreFirst:
            assert 0==1, "ignoreFirst not implemented yet."

        for k, v in self.argDict.iteritems():
            if k in requiredArgs:
                requiredMatches[k] = v
                requiredArgs.remove(k)
            elif k in optionalArgs:
                optionalMatches[k] = v
                optionalArgs.remove(k)
            else:
                leftovers.append((k,v))

        if self.debug > 3:
            CPL.log("Command.coverArgs",
                    "raw=%r requiredMatches=%r optionalMatches=%r unmatched=%r leftovers=%r" \
                    % (argDict, requiredMatches, optionalMatches, requiredArgs, leftovers))
                
        return requiredMatches, requiredArgs, optionalMatches, leftovers

if __name__ == "__main__":
    c = Command(1, 2, None, "tcmd arg1=v1 arg2=v2 arg3 arg4 arg5='v5' 3 3 5 acb=99", None)

    requiredMatches, requiredArgs, optionalMatches, leftovers = \
                     c.coverArgs(('arg1', 'xxx', 'arg4'), ('arg3', 'arg5', 'noarg'))
    requiredMatches, requiredArgs, optionalMatches, leftovers = \
                     c.coverArgs(None, None, 'tcmd')
