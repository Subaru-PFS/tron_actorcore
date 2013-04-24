#!/usr/bin/env python

import actorcore.Actor

class __ACTOR_NAME__(actorcore.Actor.Actor):
    def __init__(self, name, productName=None, configFile=None, debugLevel=30):
        # This sets up the connections to/from the hub, the logger, and the twisted reactor.
        #
        actorcore.Actor.Actor.__init__(self, name, 
                                       productName=productName, 
                                       configFile=configFile)

#
# To work
def main():
    theActor = __ACTOR_NAME__('__ACTOR_NAME__', productName='__PRODUCT_NAME__')
    theActor.run()

if __name__ == '__main__':
    main()

    
    
