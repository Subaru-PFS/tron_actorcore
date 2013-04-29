from __future__ import with_statement

__all__ = ['CommandLinkManager', 'listen']

import logging

from twisted.internet.protocol import Factory
from twisted.internet import reactor

from CommandLink import CommandLink

class CommandLinkManager(Factory):
    """ Launch an instance of the given Protocol when a new connection comes in. """

    protocol = CommandLink
    
    def __init__(self, brains, protocolName='CommandLink',
                 port=0, interface=''):
        """ Manage a dynamic set of CommandLinks. 

        Args:
           brains  - the object which operates on new Commands. Simply passed in to the
                     CommandLink objects.
                     
        We track all the protocol instances here, so that we can output replies on all active
        connections.
        """

        # Factory.__init__(self)   # snarl: twisted uses old-style classes...
        
        self.brains = brains
        self.protocolName = protocolName

        self.activeConnections = []
        self.connID = 1

        self.listeningPort = reactor.listenTCP(port, self, interface=interface)
        
    def fetchCid(self):
        """ Return the next available connection ID. """

        cid = self.connID
        self.connID += 1

        return cid

    def startFactory(self):
        pass

    def buildProtocol(self, addr):
        """ Generate a new CommandLink instance. Called when a new connection has been established. """

        # Try to allow reloading the protocol.
        #proto = eval(self.protocolName)
        #if proto != self.protocol:
        #    logging.info('changing %s class' % (self.protocolName))
        #    self.protocol = proto

        cid = self.fetchCid()
        p = self.protocol(brains=self.brains, connID=cid)
        p.factory = self

        self.activeConnections.append(p)
        return p

    def loseConnection(self, c):
        """ Unregister one of our connections. Must be called when connections close or die. """

        try:
            self.activeConnections.remove(c)
        except ValueError, e:
            raise                       # CPL
        
    def sendResponse(self, cmd, flag, response):
        """ Ship a response off to all connections. """

        for c in self.activeConnections:
            try:
                c.sendResponse(cmd, flag, response)
            except Exception, e:
                raise                   # CPL
        
def listen(actor, port, interface=''):
    """ Launch a manager listening on a given interface+port""" 
    mgr = CommandLinkManager(actor)
    p = reactor.listenTCP(port, mgr, interface=interface)
    
    return mgr

def main():
    from twisted.internet import reactor
    import sdss3logging
    
    class DummyActor(object):
        def newCmd(self, cmd):
            cmd.respond('text="new Command: %s"' % (cmd))

    actor = DummyActor()
    listen(actor, port=9999, interface='localhost') 

    logging.info("starting reactor....")
    reactor.run()
'''
    c = CommandNub()
    c.lineReceived('')
    c.lineReceived('blat')
    c.lineReceived('10 abc def')
    c.lineReceived('goof.ball 10 abc def')
    c.lineReceived('ball. 10 abc def')
    c.lineReceived('ball 10 abc def')
    c.lineReceived('abc def')
'''

if __name__ == "__main__":
    main()
    
