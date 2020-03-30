#!/usr/bin/env python3

import os
import sys
import time

from twisted.internet import reactor

import actorcore.Actor
from opscore.actor.keyvar import AllCodes, DoneCodes, FailedCodes
import opscore.actor.keyvar as opsKeyvar

class OurActor(actorcore.Actor.Actor):
    def __init__(self, name,
                 cmdActor, cmdStr,
                 printLevel,
                 printTimes=False,
                 timelim=30,
                 productName=None, configFile=None,
                 modelNames=(),
                 debugLevel=40):

        if configFile is None:
            configFile = os.path.expandvars(os.path.join('$TRON_ACTORCORE_DIR', 'etc', 'oneCmd.cfg'))

        self.cmdActor = cmdActor
        self.cmdStr = cmdStr
        self.timelim = timelim
        self.printTimes = printTimes

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
        self.logger.setLevel(self.printLevel * 10 + 5)

    def TS(self):
        now = time.time()
        basetime = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(now))
        fractime = int((now % 1) * 1000)

        return "%s.%03d " % (basetime, fractime)

    def printResponse(self, resp):
        reply = resp.replyList[-1]
        code = resp.lastCode

        if self.printLevels[code] >= self.printLevel:
            if self.printTimes:
                ts = self.TS()
            else:
                ts = ''
            print("%s%s %s %s" % (ts, reply.header.actor, reply.header.code.lower(),
                                  reply.keywords.canonical(delimiter=';')))
            sys.stdout.flush()

        if code in DoneCodes:
            self._shutdown()
            reactor.stop()

            os._exit(code in FailedCodes)

    def callAndPrint(self):
        cmdvar = opsKeyvar.CmdVar(cmdStr=self.cmdStr,
                                  actor=self.cmdActor,
                                  timeLim=self.timelim,
                                  callFunc=self.printResponse,
                                  callCodes=AllCodes)

        if self.printTimes:
            now = self.TS()
            print(f'{now}sent {self.cmdActor} {self.cmdStr}')

        reactor.callLater(0, self.cmdr.dispatcher.executeCmd, cmdvar)

    def _connectionMade(self):
        """twisted arranges to call this when self.cmdr has been established. 

        Usually, the Actor starts up by sending a command to the hub
        to connect back to us. We want to skip this step.

        """
        self.cmdr.logger.setLevel(self.printLevel * 10 + 5)
        self.bcast.inform('%s is connected to the hub.' % (self.name))
        self.callAndPrint()

#
# To work
def main(argv=None):
    import argparse
    import sys

    if argv is None:
        argv = sys.argv[1:]
    elif isinstance(argv, str):
        import shlex
        argv = shlex.split(argv)

    parser = argparse.ArgumentParser(description="Send a single actor command")
    parser.add_argument('--level', 
                        choices={'d','i','w','f'},
                        default='i',
                        help='minimum reply level [d(ebug),i(nfo),w(arn),f(ail)]. Default=i')
    parser.add_argument('--timelim', default=0.0,
                        type=float,
                        help='how long to wait for command completion. Default=0.0 (forever)')
    parser.add_argument('--noTimes', action='store_true',
                        help='print timestamps for each line.')
    parser.add_argument('actor',
                        help='actor name')
    parser.add_argument('commandAndArgs', nargs=argparse.REMAINDER,
                        help='actor command and arguments')
    opts = parser.parse_args(argv)
    actorArgs = opts.commandAndArgs

    if False:
        print('rawOpts="%s' % (opts))
        print('actorname=%s level=%s args=%s' % (opts.actor, opts.level, actorArgs))

    theActor = OurActor('oneCmd', productName='tron_actorcore',
                        cmdActor=opts.actor,
                        timelim=opts.timelim,
                        cmdStr=' '.join(actorArgs),
                        printLevel=opts.level.upper(),
                        printTimes=(not opts.noTimes))
    theActor.run()

if __name__ == '__main__':
    main()
