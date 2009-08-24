import opscore.utility.sdss3logging
import logging
import threading
import Queue
import ConfigParser

from twisted.internet import reactor
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.protocols.basic import LineReceiver

import opscore.utility as opsUtils
import opscore.actor.cmdkeydispatcher as opsDispatcher
import opscore.actor.model as opsModel
import opscore.actor.keyvar as opsKeyvar

class CmdrConnection(LineReceiver):
    def __init__(self, readCallback, brains, logger=None, **argv):
        """ The Commander twisted Protocol: sends command lines and passes on replies. 
        """

        self.delimiter = '\n'
        self.readCallback = readCallback
        self.brains = brains
        self.lock = threading.Lock()
        self.logger = logger if logger else logging.getLogger('cmdr') 

        self.logger.info('starting new CmdrConnection')
        
    def connectionMade(self):
        self.brains.connectionMade()
        
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
    def __init__(self, name, brains, logger=None):
        self.name = name
        self.cmdr = name
        self.brains = brains
        self.readCallback = None
        self.stateCallback = None

        self.maxDelay = 60
        self.initialDelay = 0.5
        self.factor = 2
        
        # We can only have one connection...
        self.activeConnection = None

        self.logger = logger if logger else logging.getlogger('cmdr')

    def doStart(self):
        self.logger.warn("in doStart")
        
    def buildProtocol(self, addr):
        """ A new connection has been established. Create a new Protocol. """

        self.logger.info('launching new CmdrConnection')

        assert (self.activeConnection == None), "connection already active!"
        assert (self.readCallback != None), "readCallback has not yet been set!"
        
        self.resetDelay()
        proto = CmdrConnection(self.readCallback, brains=self.brains, logger=self.logger)
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

        self.logger.warn('writing %s' % (cmdStr))
        if not self.activeConnection:
            raise RuntimeError("not connected.")
        self.activeConnection.write(cmdStr + '\n')

class Cmdr(object):
    def __init__(self, name, actor, loggerName='cmdr'):
        self.actor = actor

        logger = logging.getLogger(loggerName)
        self.logger = logger
        
        self.connector = CmdrConnector(name, self, logger=logger)
        self.factory = self.connector

        # Start a dispatcher, connected to our logger. Wire the dispatcher
        # in to the Model "singleton"
        logger = logging.getLogger('dispatch')
        def logFunc(msgStr, severity, actor, cmdr, logger=logger):
            logger.info("%s %s %s %s" % (cmdr, actor, severity, msgStr))

        self.dispatcher = opsDispatcher.CmdKeyVarDispatcher(self.actor, self.connector, 
                                                            logFunc, includeName=True)
        opsModel.Model.setDispatcher(self.dispatcher)

    def connectionMade(self):
        pass
    
    def connect(self):
        tronHost = self.actor.config.get('tron', 'tronHost')
        tronPort = int(self.actor.config.get('tron', 'tronCmdrPort'))
        
        reactor.connectTCP(tronHost, tronPort, self.connector)

    def call(self, **argv):
        self.logger.info("sending command %s" % (argv))

        q = Queue.Queue()
        argv['callFunc'] = q.put
        cmdvar = opsKeyvar.CmdVar(**argv)
        self.dispatcher.executeCmd(cmdvar)
        ret = q.get()

        self.logger.info("command %s returned " % (cmdvar))
        
        return ret
        
    def waitForKey(self, **argv):
        logging.info("sending command %s" % (argv))

        q = Queue.Queue()
        argv['callFunc'] = q.put
        cmdvar = opsKeyvar.KeyVar(**argv)
        self.dispatcher.executeCmd(cmdvar)
        ret = q.get()

        logging.info("command %s returned " % (cmdvar))
        
        return ret
        
def liveTest():
    """ Connect to a running hub and print out all tcc traffic. """
    import opscore.utility.sdss3logging
    import opscore.actor.keyvar as keyvar
    
    logger = logging.getLogger('test')

    def showVal(keyVar, logger=logger):
        logger.info("keyVar %s.%s = %r, isCurrent = %s" % (keyVar.actor, keyVar.name, keyVar.valueList, keyVar.isCurrent))
                
    cmdr = Cmdr('test.me')
    cmdr.connect()

    # Register all tcc keywords to be printed.
    tccModel = opsModel.Model('tcc')
    for o in tccModel.__dict__.values():
        if isinstance(o, keyvar.KeyVar):
            o.addCallback(showVal)

    reactor.run()

def main():
    liveTest()
    
if __name__ == "__main__":
    main()
                       
