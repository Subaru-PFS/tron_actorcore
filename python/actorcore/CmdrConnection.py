from __future__ import with_statement

import opscore.utility.sdss3logging
import logging
import threading

from opscore.utility.qstr import qstr
from opscore.utility.tback import tback

from twisted.internet import reactor
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.protocols.basic import LineReceiver

from opscore.actor.cmdkeydispatcher import CmdKeyVarDispatcher

class CmdrConnection(LineReceiver):
    def __init__(self, readCallback, **argv):
        """ The Commander Protocol: sends command lines and passes on replies. 
        """

        self.delimiter = '\n'
        self.readCallback = readCallback
        self.lock = threading.Lock()
        self.logger = logging.getLogger()

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
    def __init__(self, name):
        self.name = name
        self.cmdr = name
        self.readCallback = None
        self.stateCallback = None

        self.maxDelay = 60
        self.initialDelay = 0.5
        self.factor = 2

        # We can only have one connection...
        self.activeConnection = None

    def buildProtocol(self, addr):
        """ A new connection has been established. Create a new Protocol. """

        logging.info('launching new CmdrConnection')

        assert (self.activeConnection == None), "connection already active!"
        assert (self.readCallback != None), "readCallback has not yet been set!"
        
        self.resetDelay()
        proto = CmdrConnection(self.readCallback)
        proto.factory = self
        self.activeConnection = proto
        self.stateCallback(self)
        
        return proto
    
    def clientConnectionLost(self, connector, reason):
        """ We are called when our connection is lost. Start trying to create a new connection. """

        logging.warn('hub connection lost: %s ' % reason)
        
        self.activeConnection = None
        self.stateCallback(self)
        ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):

        logging.warn('hub connection failed: %s ' % (reason))

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

        if not self.activeConnection:
            raise RuntimeError("not connected.")
        self.activeConnection.write(cmdStr + '\n')

def logFunc(msgStr, severity, actor, cmdr):
    logging.info("%s %s %s %s" % (cmdr, actor, severity, msgStr))

def setupCmdr(name, host, port):
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    conn = CmdrConnector(name)
    dispatcher = CmdKeyVarDispatcher(name, conn, logFunc)
    reactor.connectTCP(host, port, conn)

    return conn, dispatcher

def liveTest():
    import opscore.utility.sdss3logging
    import opscore.actor.model as opsModel
    import opscore.actor.keyvar as keyvar
    
    def showVal(keyVar):
        print "keyVar %s.%s = %r, isCurrent = %s" % (keyVar.actor, keyVar.name, keyVar.valueList, keyVar.isCurrent)
                
    conn, dispatcher = setupCmdr('test.me', 'localhost', 6093)
    opsModel.Model.setDispatcher(dispatcher)
    tccModel = opsModel.Model('tcc')
    
    keyVarList = [tccModel.__dict__[key] for key in ('tccStatus', 'axePos', 'secOrient')]
    for keyVar in keyVarList:
        keyVar.addCallback(showVal)

    reactor.run()

def main():
    liveTest()
    
if __name__ == "__main__":
    main()
                       
