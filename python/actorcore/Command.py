""" Command.py -- essentials of hub commands.

    When commands come in from the hub or some other source, a Command
    instance is created to shepherd it though to completion. A
    Command knows what the command is and where it came from, and provides
    wrappers for responding to the sender.
    
"""
from builtins import object
__all__ = ['Command']

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
        
        self.alive = True
        self.immortal = immortal
 
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

        self.inform(response)

    def inform(self, response, warn=False):
        """ Return intermediate response. """

        self.__respond('i' if not warn else 'w', response)
    info = inform
    
    def diag(self, response):
        """ Return diagnostic output. """

        self.debug(response)
        
    def debug(self, response):
        """ Return diagnostic output. """

        self.__respond('d', response)
        
    def warn(self, response):
        """ Return warning. """

        self.__respond('w', response)
        
    def error(self, response):
        """ Return intermediate error response. """

        self.__respond('e', response)
        
    def finish(self, response='', fail=False):
        """ Return (usually) successful command finish. 

        Args
        ---
        response : str
           A preformatted string, of keywords. No validity checking is done.
        fail : bool
           Syntactic sugar: if True, then finish with a failure.

        """

        if fail:
            return self.fail(response)
            
        if self.immortal:
            self.__respond('i', response)
        else:
            self.__respond(':', response)
            self.alive = False
        
    def fail(self, response):
        """ Return failure. """

        if self.immortal:
            self.__respond('w', response)
        else:
            self.__respond('f', response)
            self.alive = False
        
    def sendResponse(self, flag, response):
        """ Return a response with a specific flag. """

        self.__respond(flag, response)
        
    def __respond(self, flag, response):
        """ Actually send the response to the appropriate source. If the command has already
            been finished, try broadcasting a complaint. It is a bit unclear what to do about the
            original response; I'm just passing it along to bother others. """

        if not self.alive:
            self.source.sendResponse(self, 'w', 
                                     'text="this command has already been finished!!!! (%s %s): %s"' % 
                                     (self.cmdr, self.mid, self.rawCmd))
        self.source.sendResponse(self, flag, response)
        # self.actor.bcast.warn('text="sent a response to an already finished command: %s"' % (self))
