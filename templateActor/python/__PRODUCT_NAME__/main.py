#!/usr/bin/env python3

import actorcore.Actor

class OurActor(actorcore.Actor.Actor):
    def __init__(self, name,
                 productName=None, configFile=None,
                 modelNames=(),
                 debugLevel=30):

        """ Setup an Actor instance. See help for actorcore.Actor for details. """

        # This sets up the connections to/from the hub, the logger, and the twisted reactor.
        #
        actorcore.Actor.Actor.__init__(self, name,
                                       productName=productName,
                                       configFile=configFile,
                                       modelNames=modelNames)

        # At this

#
# To work
def main():
    theActor = OurActor('__ACTOR_NAME__', productName='__PRODUCT_NAME__')
    theActor.run()

if __name__ == '__main__':
    main()
