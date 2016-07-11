#!/usr/bin/env python

import os
import threading

from twisted.internet import reactor

import actorcore.Actor
from opscore.actor.keyvar import AllCodes, DoneCodes
import opscore.actor.keyvar as opsKeyvar

class OurActor(actorcore.Actor.Actor):
    def __init__(self, name,
                 cmdActor, cmdStr,
                 printLevel,
                 timelim=30,
                 productName=None, configFile=None,
                 modelNames=(),
                 debugLevel=40):

        if configFile is None:
            configFile = os.path.expandvars(os.path.join('$TRON_ACTORCORE_DIR', 'etc', 'oneCmd.cfg'))
            
        self.cmdActor = cmdActor
        self.cmdStr = cmdStr
        self.timelim = timelim

        self.printLevels = {'D':0, '>':0,
                            'I':1, ':':1,
                            'W':2,
                            'F':3, '!':4}
        self.printLevel = self.printLevels[printLevel]
        
        # This sets up the connections to/from the hub, the logger, and the twisted reactor.
        #
        actorcore.Actor.Actor.__init__(self, name, 
                                       productName=productName, 
                                       configFile=configFile,
                                       acceptCmdrs=False,
                                       modelNames=modelNames)
        self.logger.setLevel(debugLevel)
                                    
    def printResponse(self, resp):
        reply = resp.replyList[-1]
        code = resp.lastCode

        if self.printLevels[code] >= self.printLevel:
            print("%s %s %s" % (reply.header.actor, reply.header.code.lower(),
                                reply.keywords.canonical(delimiter=';')))
            
        if code in DoneCodes:
            self._shutdown()
            reactor.stop()

    def callAndPrint(self):
        cmdvar = opsKeyvar.CmdVar(cmdStr=self.cmdStr,
                                  actor=self.cmdActor,
                                  timeLim=self.timelim,
                                  callFunc=self.printResponse,
                                  callCodes=AllCodes)

        reactor.callLater(0, self.cmdr.dispatcher.executeCmd, cmdvar)
        
    def _connectionMade(self):
        """twisted arranges to call this when self.cmdr has been established. 

        Usually, the Actor starts up by sending a command to the hub
        to connect back to us. We want to skip this step.

        """

        self.bcast.inform('%s is connected to the hub.' % (self.name))
        self.callAndPrint()

#
# To work
def main(argv=None):
    import argparse
    import sys
    
    if argv is None:
        argv = sys.argv[1:]
    elif isinstance(argv, basestring):
        import shlex
        argv = shlex.split(argv)

    actorName = os.path.basename(sys.argv[1])
    argv = argv[1:]
    
    parser = argparse.ArgumentParser(prog=actorName,
                                     description="Send a single actor command")
    parser.add_argument('--level', 
                        choices={'d','i','w','f'},
                        default='i',
                        help='minimum reply level [d(ebug),i(nfo),w(arn),f(ail)]. Default=i')
    parser.add_argument('--timelim', default=0.0,
                        help='how long to wait for command completion. 0=forever')
    parser.add_argument('commandArgs', nargs=argparse.REMAINDER)
    opts = parser.parse_args(argv)
    actorArgs = opts.commandArgs

    # print('rawOpts="%s' % (opts))
    # print('actorname=%s level=%s args=%s' % (actorName, opts.level, actorArgs))

    theActor = OurActor('oneCmd', productName='tron_actorcore',
                        cmdActor=actorName,
                        timelim=opts.timelim,
                        cmdStr=' '.join(actorArgs),
                        printLevel=opts.level.upper())
    theActor.run()

if __name__ == '__main__':
    main()
