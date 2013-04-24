#!/usr/bin/env python

import actorcore.Actor
import fakeCamera

class Mcs(actorcore.Actor.Actor):
    def __init__(self, name, productName=None, configFile=None, debugLevel=30):
        # This sets up the connections to/from the hub, the logger, and the twisted reactor.
        #
        actorcore.Actor.Actor.__init__(self, name, 
                                       productName=productName, 
                                       configFile=configFile)


        # We will actually use a allocator with "global" sequencing
        self.exposureID = 0
        
        self.connectCamera(self.bcast)
        self.filterName = 'J'

    def connectCamera(self, cmd, doFinish=True):
        """ Create a new connection to our camera. Destroy any existing connection. """

        reload(fakeCamera)
        self.camera = fakeCamera.FakeCamera()
        self.camera.sendStatusKeys(cmd)

    def setFilter(self, cmd, filterName):
        """ Create a new connection to our camera. Destroy any existing connection. """

        self.filterName = filterName
        self.finish('filter=%s' % (self.filterName))
        

#
# To work
def main():
    mcs = Mcs('mcs', productName='mcsActor')
    mcs.run()

if __name__ == '__main__':
    main()

    
    
