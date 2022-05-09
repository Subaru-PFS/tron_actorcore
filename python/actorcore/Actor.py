import configparser
import imp
import inspect
import logging
import os
import queue
import re
import sys
import threading
import traceback

# This has to be set very early -- earlier than some imports, even.
import RO.Comm.Generic
# import our routines before logging itself.
import opscore.utility.sdss3logging as opsLogging
from twisted.internet import reactor

RO.Comm.Generic.setFramework('twisted')

import eups

import opscore
from opscore.protocols.parser import CommandParser
from opscore.utility.qstr import qstr
from opscore.utility.tback import tback

import opscore.protocols.keys as keys
import opscore.protocols.validation as validation

from . import CommandLinkManager as cmdLinkManager
from . import Command as actorCmd
from . import CmdrConnection

from ics.utils import versions
import ics.utils.instdata as instdata


class Actor(object):
    def __init__(self, name, productName=None, configFile=None,
                 makeCmdrConnection=True,
                 acceptCmdrs=True,
                 productPrefix="",
                 modelNames=()):
        """ Build an Actor.

        Args:
            name         - the name of the actor: what name we are advertised as to tron.
            productName  - the name of the product; defaults to .name
            configFile   - the full path of the configuration file; defaults
                            to $PRODUCTNAME_DIR/etc/$name.cfg
            makeCmdrConnection
                         - establish self.cmdr as a command connection to the hub.
                           This needs to be True if we send commands or listen to keys.
            acceptCmdrs  - allow incoming commander connections.
            modelNames   - a list of actor names, whose key dictionaries we want to
                           listen to and have in our .models[]
        """

        # Define/save the actor name, the product name, the product_DIR, and the
        # configuration file.
        self.name = name
        self.productName = productName if productName else self.name

        # Incomplete/missing eups environment will make us blow
        # up. Note that we allow "actorName" as equivalent to
        # "ics_actorName". This was a bad decision made a long time
        # ago.
        eupsEnv = eups.Eups()
        setupProds = eupsEnv.findProducts(tags='setup', name=self.productName)
        if len(setupProds) == 0:
            setupProds = eupsEnv.findProducts(tags='setup', name=f'ics_{self.productName}')
        if len(setupProds) == 0:
            raise RuntimeError(f"cannot figure out what our eups product name is. {self.productName} is not useful")

        self.product_dir = setupProds[0].dir
        self.configFile = configFile if configFile else \
            os.path.expandvars(os.path.join(self.product_dir, 'etc', '%s.cfg' % (self.name)))

        # Missing config bits should make us blow up.
        self.configFile = os.path.expandvars(self.configFile)

        self._reloadConfiguration()
        self.logger.info('%s starting up....' % (name))
        self.parser = CommandParser()

        self.acceptCmdrs = acceptCmdrs

        self.models = {}
        if modelNames and not makeCmdrConnection:
            self.logger.warn("modelNames were requested but makeCmdrConnection is False. Forcing that to True.")
            makeCmdrConnection = True

        # The list of all connected sources.
        listenInterface = self.actorConfig['listen']['interface']
        if listenInterface == 'None':
            listenInterface = None
        listenPort = self.actorConfig['listen']['port']

        # Allow for a passive actor which does not accept external commands.
        if not self.acceptCmdrs:
            listenInterface = None
        self.commandSources = cmdLinkManager.CommandLinkManager(self,
                                                                port=listenPort,
                                                                interface=listenInterface)
        # IDs to send commands to ourself.
        self.selfCID = self.commandSources.fetchCid()
        self.synthMID = 1

        # The Command which we send uncommanded output to.
        self.bcast = actorCmd.Command(self.commandSources,
                                      'self.0', 0, 0, None, immortal=True)

        # commandSets are the command handler packages. Each handles
        # a vocabulary, which it registers when loaded.
        # We gather them in one place mainly so that "meta-commands" (init, status)
        # can find the others.
        self.commandSets = {}
        if acceptCmdrs:
            self.logger.info("Creating validation handler...")
            self.handler = validation.CommandHandler()

            self.logger.info("Attaching actor command sets...")
            self.attachAllCmdSets()
            self.logger.info("All command sets attached...")

            self.commandQueue = queue.Queue()
        self.shuttingDown = False

        if makeCmdrConnection:
            self.cmdr = CmdrConnection.Cmdr(name, self)
            self.cmdr.connectionMade = self._connectionMade
            self.cmdr.connectionLost = self.connectionLost
            self.cmdr.connect()
            self._initializeHubModels(modelNames)
        else:
            self.cmdr = None

    def _reloadConfiguration(self, cmd=None):
        logging.info("reading config file %s", self.configFile)

        try:
            newConfig = configparser.ConfigParser()
            newConfig.read(self.configFile)
            self.config = newConfig
        except Exception as e:
            if cmd:
                cmd.warn('text=%s' % (qstr("failed to read the configuration file, old config untouched: %s" % (e))))

        try:
            newConfig = instdata.InstConfig(self.name)
        except Exception as e:
            if cmd:
                cmd.fail('text=%s' % (qstr("failed to load instdata configuration file, old config untouched: %s" % (e))))
            raise

        self.actorConfig = newConfig
        self.configureLogs()

        try:
            # Call optional user hook.
            self.reloadConfiguration(cmd)
        except:
            pass

    def configureLogs(self, cmd=None):
        """ (re-)configure our logs. """
        loggingConfig = self.actorConfig['logging']
        # per actor log dir.
        self.logDir = loggingConfig['logdir']
        if not self.logDir:
            raise RuntimeError("logdir must be set!")

        # Make the root logger go to a rotating file. All others derive from this.
        opsLogging.setupRootLogger(self.logDir)

        # The real stderr/console filtering is actually done through the console Handler.
        try:
            consoleLevel = loggingConfig['consoleLevel']
        except:
            consoleLevel = loggingConfig['baseLevel']

        opsLogging.setConsoleLevel(consoleLevel)

        # self.console needs to be renamed ore deleted, I think.
        self.console = logging.getLogger('')
        self.console.setLevel(loggingConfig['baseLevel'])

        self.logger = logging.getLogger('actor')
        self.logger.setLevel(loggingConfig['baseLevel'])
        self.logger.propagate = True
        self.logger.info('(re-)configured root and actor logs')

        self.cmdLog = logging.getLogger('cmds')
        self.cmdLog.setLevel(loggingConfig['cmdLevel'])
        self.cmdLog.propagate = True
        self.cmdLog.info('(re-)configured cmds log')

        if cmd:
            cmd.inform('text="reconfigured logs"')

    def versionString(self, cmd):
        """ Return the version key value.

        If you simply want to generate the keyword, call .sendVersionKey().
        """

        versionString = versions.version('ics_' + self.productName)
        if versionString == "unknown" or versionString == "":
            versionString = versions.version(self.productName)
        if versionString == "unknown" or versionString == "":
            cmd.warn("text='pathetic version string: %s'" % (versionString))

        return versionString

    def sendVersionKey(self, cmd):
        """ Generate the version keyword in response to cmd. """

        setupVersions = versions.allSetupVersions()
        for prodName in setupVersions.keys():
            versionString = setupVersions[prodName]
            cmd.inform(f'version_{prodName}=%s' % qstr(versionString))

        versionString = self.versionString(cmd)
        cmd.inform('version=%s' % (qstr(versionString)))

    def triggerHubConnection(self):
        """ Send the hub a command to connect back to us. """

        if not self.cmdr:
            self.bcast.warn('text="CANNOT ask hub to connect to us, since we do not have a connection to it yet!"')
            return

        if not self.acceptCmdrs:
            self.logger.warn('not triggering hub callback.')
            return

        # Look for override on where tron should connect back to. For tunnels, etc.
        try:
            advertisedSocket = self.actorConfig['listen']['hostAndPortForTron']
            ourHost, ourPort = advertisedSocket.split(':')
        except:
            ourAddr = self.commandSources.listeningPort.getHost()
            ourPort = ourAddr.port
            ourHost = ourAddr.host

        cmdStr = "startNub %s %s:%s" % (self.name, ourHost, ourPort)
        self.logger.info('text=%s' % (qstr("asking the hub to connect back to us with: %s" % (cmdStr))))
        self.cmdr.dispatcher.executeCmd(opscore.actor.keyvar.CmdVar(actor='hub',
                                                                    cmdStr=cmdStr,
                                                                    timeLim=5.0))

    def _initializeHubModels(self, modelNames):
        """ Initialize the dictionary of models we want to track. """

        if isinstance(modelNames, str):
            modelNames = [modelNames]

        for n in modelNames:
            self.models[n] = opscore.actor.model.Model(n)

    def _initializeHubInterest(self):
        """ Tell the hub which actors we want updates from. """

        if not self.cmdr:
            self.bcast.warn('text="CANNOT ask hub to connect to us, since we do not have a connection to it yet!"')
            return

        modelNames = list(self.models.keys())
        actorString = " ".join(modelNames)
        self.bcast.warn('%s is asking the hub to send us updates from %s' % (self.name, modelNames))
        self.cmdr.dispatcher.executeCmd(opscore.actor.keyvar.CmdVar(actor='hub',
                                                                    cmdStr='listen clearActors',
                                                                    timeLim=5.0))
        if modelNames:
            self.cmdr.dispatcher.executeCmd(opscore.actor.keyvar.CmdVar(actor='hub',
                                                                        cmdStr='listen addActors %s' % (actorString),
                                                                        timeLim=5.0))

    def addModels(self, newModelNames):
        """ Add new models/actors to get and keep keyword updates from.

        Args
        ====
        newModelNames : list-like or string
           names of actors to start listening to.
        """

        if isinstance(newModelNames, str):
            newModelNames = [newModelNames]

        for n in newModelNames:
            if n in self.models:
                self.bcast.warn('text="model %s is already being tracked"' % (n))
            else:
                try:
                    self.models[n] = opscore.actor.model.Model(n)
                    self.cmdr.dispatcher.executeCmd(opscore.actor.keyvar.CmdVar(actor='hub',
                                                                                cmdStr='listen addActors %s' % (n),
                                                                                timeLim=5.0))
                except Exception as e:
                    self.logger.warn('text="failed to add model %s: %s"' % (n, e))
                    self.bcast.warn('text="failed to add model %s: %s"' % (n, e))

    def dropModels(self, dropModelNames):
        """ Add new models/actors to get and keep keyword updates from.

        Args
        ====
        dropModelNames : list-like or string
           names of actors to start listening to.
        """

        if isinstance(dropModelNames, str):
            dropModelNames = [dropModelNames]

        for n in dropModelNames:
            if n not in self.models:
                self.bcast.warn('text="model %s is not currently being tracked"' % (n))
            else:
                try:
                    del self.models[n]
                    self.cmdr.dispatcher.executeCmd(opscore.actor.keyvar.CmdVar(actor='hub',
                                                                                cmdStr='listen delActors %s' % (n),
                                                                                timeLim=5.0))
                except Exception as e:
                    self.bcast.warn('text="failed to drop model %s: %s"' % (n, e))

    def _connectionMade(self):
        """ twisted arranges to call this when self.cmdr has been established. """

        self.bcast.warn('%s is connected to the hub.' % (self.name))

        # Tell the hub to keep us updated with keys from the models we are interested in.
        self._initializeHubInterest()

        # Request that tron connect to us.
        self.triggerHubConnection()
        self.connectionMade()

    def connectionMade(self):
        """ For overriding. """
        pass

    def connectionLost(self, reason):
        """ For overriding. """
        pass

    def attachCmdSet(self, cname, path=None):
        """ (Re-)load and attach a named set of commands. """

        if path is None:
            path = [os.path.join(self.product_dir, 'python', self.name, 'Commands')]

        self.logger.info("attaching command set %s from path %s", cname, path)

        file = None
        try:
            file, filename, description = imp.find_module(cname, path)
            self.logger.debug("command set file=%s filename=%s from path %s",
                              file, filename, path)
            mod = imp.load_module(cname, file, filename, description)
        except ImportError as e:
            raise RuntimeError('Import of %s failed: %s' % (cname, e))
        finally:
            if file:
                file.close()

        # Instantiate and save a new command handler.
        cmdClass = getattr(mod, cname)
        cmdSet = cmdClass(self)

        # pdb.set_trace()

        # Check any new commands before finishing with the load. This
        # is a bit messy, as the commands might depend on a valid
        # keyword dictionary, which also comes from the module
        # file.
        #
        # BAD problem here: the Keys define a single namespace. We need
        # to check for conflicts and allow unloading. Right now we unilaterally
        # load the Keys and do not unload them if the validation fails.
        if hasattr(cmdSet, 'keys') and cmdSet.keys:
            keys.CmdKey.addKeys(cmdSet.keys)
        valCmds = []
        for v in cmdSet.vocab:
            try:
                verb, args, func = v
            except ValueError as e:
                raise RuntimeError("vocabulary word needs three parts: %s" % (repr(v)))

            # Check that the function exists and get its help.
            #
            funcDoc = inspect.getdoc(func)
            valCmd = validation.Cmd(verb, args, help=funcDoc) >> func
            valCmds.append(valCmd)

        # Got this far? Commit. Save the Cmds so that we can delete them later.
        oldCmdSet = self.commandSets.get(cname, None)
        cmdSet.validatedCmds = valCmds
        self.commandSets[cname] = cmdSet

        # Delete previous set of consumers for this named CmdSet, add new ones.
        if oldCmdSet:
            self.handler.removeConsumers(*oldCmdSet.validatedCmds)
        self.handler.addConsumers(*cmdSet.validatedCmds)

        self.logger.warn("handler verbs: %s" % (list(self.handler.consumers.keys())))

    def attachAllCmdSets(self, path=None):
        """ (Re-)load all command classes -- files in ./Command which end with Cmd.py.
        """

        if path is None:
            self.attachAllCmdSets(path=os.path.join(os.path.expandvars('$TRON_ACTORCORE_DIR'),
                                                    'python', 'actorcore', 'Commands'))
            self.attachAllCmdSets(path=os.path.join(self.product_dir, 'python',
                                                    self.productName, 'Commands'))
            self.attachAllCmdSets(path=os.path.join(self.product_dir, 'python', 'ics',
                                                    self.productName, 'Commands'))
            return

        try:
            dirlist = os.listdir(path)
        except OSError as e:
            self.logger.warn("no Cmd path %s" % (path))
            return

        dirlist.sort()
        self.logger.warn("loading %s" % (dirlist))

        for f in dirlist:
            if os.path.isdir(f) and not f.startswith('.'):
                self.attachAllCmdSets(path=f)
            if re.match('^[a-zA-Z][a-zA-Z0-9_-]*Cmd\.py$', f):
                self.attachCmdSet(f[:-3], [path])

    def cmdTraceback(self, e):
        eType, eValue, eTraceback = sys.exc_info()
        tbList = traceback.extract_tb(eTraceback)
        where = tbList[-1]

        return "%r in %s() at %s:%d" % (eValue, where[2], where[0], where[1])

    def strTraceback(self, e):

        oneLiner = self.cmdTraceback(e)
        return qstr("command failed: %s" % oneLiner)

    def runActorCmd(self, cmd):
        try:
            cmdStr = cmd.rawCmd
            if self.cmdLog.level <= logging.DEBUG:
                self.cmdLog.debug('raw cmd: %s' % (cmdStr))

            try:
                validatedCmd, cmdFuncs = self.handler.match(cmdStr)
            except Exception as e:
                cmd.fail('text=%s' % (qstr("Unmatched command: %s (exception: %s)" %
                                           (cmdStr, e))))
                # tback('actor_loop', e)
                return

            if not validatedCmd:
                cmd.fail('text=%s' % (qstr("Unrecognized command: %s" % (cmdStr))))
                return

            self.cmdLog.debug('< %s:%d %s' % (cmd.cmdr, cmd.mid, validatedCmd))
            if len(cmdFuncs) > 1:
                cmd.warn('text=%s' % (qstr("command has more than one callback (%s): %s" %
                                           (cmdFuncs, validatedCmd))))
            try:
                cmd.cmd = validatedCmd
                for func in cmdFuncs:
                    func(cmd)
            except Exception as e:
                oneLiner = self.cmdTraceback(e)
                cmd.fail('text=%s' % (qstr("command failed: %s" % (oneLiner))))
                # tback('newCmd', e)
                return

        except Exception as e:
            cmd.fail('text=%s' % (qstr("completely unexpected exception when processing a new command: %s" %
                                       (e))))
            try:
                tback('newCmdFail', e)
            except:
                pass

    def actor_loop(self):
        """ Check the command queue and dispatch commands."""

        while True:
            try:
                cmd = self.commandQueue.get(block=True, timeout=3)
            except queue.Empty:
                if self.shuttingDown:
                    return
                else:
                    continue
            self.runActorCmd(cmd)

    def commandFailed(self, cmd):
        """ Gets called when a command has failed. """
        pass

    def newCmd(self, cmd):
        """ Dispatch a newly received command. """

        if self.cmdLog.level <= logging.DEBUG:
            self.cmdLog.debug('new cmd: %s' % (cmd))
        else:
            dcmd = cmd.rawCmd if len(cmd.rawCmd) < 80 else cmd.rawCmd[:80] + "..."
            self.cmdLog.info('new cmd: %s' % (dcmd))

        # Empty cmds are OK; send an empty response...
        if len(cmd.rawCmd) == 0:
            cmd.finish('')
            return None

        if self.runInReactorThread:
            self.runActorCmd(cmd)
        else:
            self.commandQueue.put(cmd)

        return self

    def callCommand(self, cmdStr):
        """ Send ourselves a command. """

        cmd = actorCmd.Command(self.commandSources, 'self.%d' % (self.selfCID),
                               cid=self.selfCID, mid=self.synthMID, rawCmd=cmdStr)
        self.synthMID += 1
        self.newCmd(cmd)

    def _shutdown(self):
        self.shuttingDown = True

    def run(self, doReactor=True):
        """ Actually run the twisted reactor. """
        try:
            self.runInReactorThread = self.actorConfig['misc']['runInReactorThread']
        except:
            self.runInReactorThread = False

        self.logger.info("starting reactor (in own thread=%s)...." % (not self.runInReactorThread))
        try:
            if not self.runInReactorThread:
                actorThread = threading.Thread(target=self.actor_loop)
                actorThread.start()
            if doReactor:
                reactor.run()
        except Exception as e:
            tback('run', e)

        if doReactor:
            self.logger.info("reactor dead, cleaning up...")
            self._shutdown()
