from __future__ import with_statement

import opscore.utility.sdss3logging
import logging
import threading

from opscore.utility.qstr import qstr
from opscore.utility.tback import tback

from twisted.internet import reactor
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.protocols.basic import LineReceiver

from opscore.actor.cmdkeydispatcher import CmdKeyVarDispatcher as opsDispatcher
import opscore.actor.model as opsModel

class CmdrConnection(LineReceiver):
    def __init__(self, readCallback, logger=None, **argv):
        """ The Commander twisted Protocol: sends command lines and passes on replies. 
        """

        self.delimiter = '\n'
        self.readCallback = readCallback
        self.lock = threading.Lock()
        self.logger = logger if logger else logging.getLogger('cmdr') 

        self.logger.info('starting new CmdrConnection')
        
    def write(self, cmdStr):
        """ Main entry point for sending a command.

        Args:
           c        - a Command to send
           debug    -
           timeout  - number of seconds to wait before failing the command.
        """
        
        with self.lock:
            self.logger.info("sending command %s" % (cmdStr))
            self.transport.write(cmdStr)

    def lineReceived(self, replyStr):
        """ Incorporate an entire reply line.

        Args:
           replyStr   - the new reply line.
        """

        self.logger.debug('read: ' + replyStr)
        self.readCallback(self.transport, replyStr)

class CmdrConnector(ReconnectingClientFactory):
    def __init__(self, name, logger=None):
        self.name = name
        self.cmdr = name
        self.readCallback = None
        self.stateCallback = None

        self.maxDelay = 60
        self.initialDelay = 0.5
        self.factor = 2
        
        # We can only have one connection...
        self.activeConnection = None

        self.logger = logger if logger else logging.getlogger('cmdr')
        
    def buildProtocol(self, addr):
        """ A new connection has been established. Create a new Protocol. """

        self.logger.info('launching new CmdrConnection')

        assert (self.activeConnection == None), "connection already active!"
        assert (self.readCallback != None), "readCallback has not yet been set!"
        
        self.resetDelay()
        proto = CmdrConnection(self.readCallback, logger=self.logger)
        proto.factory = self
        self.activeConnection = proto
        self.stateCallback(self)
        
        return proto
    
    def clientConnectionLost(self, connector, reason):
        """ We are called when our connection is lost. Start trying to create a new connection. """

        self.logger.warn('CmdrConnection lost: %s ' % reason)
        
        self.activeConnection = None
        self.stateCallback(self)
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):

        self.logger.warn('CmdrConnection failed: %s ' % (reason))

        self.activeConnection = None
        self.stateCallback(self)
        ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)

    def isConnected(self):
        """ Called by the dispatcher to find out if we are connected."""
        return self.activeConnection != None

    def addReadCallback(self, readCallback):
        self.readCallback = readCallback

    def addStateCallback(self, stateCallback):
        self.stateCallback = stateCallback
        
    def writeLine(self, cmdStr):
        """ Called by the dispatcher to send a command. """
        
        if not self.activeConnection:
            raise RuntimeError("not connected.")
        self.activeConnection.write(cmdStr + '\n')

def setupCmdr(name, loggerName='cmdr'):
    logger = logging.getLogger(loggerName)
    logger.setLevel(logging.INFO)
    conn = CmdrConnector(name, logger=logger)

    logger = logging.getLogger('dispatch')
    logger.setLevel(logging.INFO)

    def logFunc(msgStr, severity, actor, cmdr, logger=logger):
        logger.info("%s %s %s %s" % (cmdr, actor, severity, msgStr))

    dispatcher = opsDispatcher(name, conn, logFunc)
    opsModel.Model.setDispatcher(dispatcher)
    return conn, dispatcher

def liveTest():
    """ Connect to a running hub and print out all tcc traffic. """
    import opscore.utility.sdss3logging
    import opscore.actor.keyvar as keyvar
    
    logger = logging.getLogger('test')
    logger.setLevel(logging.INFO)

    def showVal(keyVar, logger=logger):
        logger.info("keyVar %s.%s = %r, isCurrent = %s" % (keyVar.actor, keyVar.name, keyVar.valueList, keyVar.isCurrent))
                
    conn, dispatcher = setupCmdr('test.me')
    reactor.connectTCP('localhost', 6093, conn)

    opsModel.Model.setDispatcher(dispatcher)
    tccModel = opsModel.Model('tcc')

    # Register all tcc keywords to be printed.
    for o in tccModel.__dict__.values():
        if isinstance(o, keyvar.KeyVar):
            o.addCallback(showVal)

    reactor.run()

def main():
    liveTest()
    
if __name__ == "__main__":
    main()
                       
