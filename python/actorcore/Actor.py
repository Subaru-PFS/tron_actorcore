import imp
import re
import sys
import opscore.utility.sdss3logging as opsLogging
import logging
import os
import Queue
import time
import ConfigParser
import threading
import inspect

from threading import Semaphore,Timer
from twisted.internet import reactor

from opscore.protocols.parser import CommandParser
from opscore.utility.qstr import qstr
from opscore.utility.tback import tback

import opscore.protocols.keys as keys
import opscore.protocols.validation as validation

import CommandLinkManager as cmdLinkManager
import Command as actorCmd

import pdb

class ModLoader():
    def load_module(self, fullpath, name):
        """ Try to load a named module in the given path. 
 
        The imp.find_module() docs give the following boring prescription: 
 
          'This function does not handle hierarchical module names 
          (names containing dots). In order to find P.M, that is, 
          submodule M of package P, use find_module() and 
          load_module() to find and load package P, and then use 
          find_module() with the path argument set to P.__path__. When 
          P itself has a dotted name, apply this recipe recursively.' 
         """

        self.icclog.info("trying to load module path=%s name=%s", fullpath, name)

        parts = fullpath.split('.')
        path = None
        while len(parts) > 1:
            pname = parts[0]
            try:
                self.icclog.debug("pre-loading path=%s pname=%s", path, pname)
                file, filename, description = imp.find_module(pname, path)

                self.icclog.debug("pre-loading package file=%s filename=%s description=%s",
                                  file, filename, description)
                mod = imp.load_module(pname, file, filename, description)
                path = mod.__path__
                parts = parts[1:]
            except ImportError, e:
                raise
            finally:
                file.close()

        try:
            self.icclog.debug("trying to find path=%s class=%s",
                              path, name)
            file, filename, description = imp.find_module(name, path)

            self.icclog.debug("trying to attach file=%s filename=%s description=%s",
                              file, filename, description)
            mod = imp.load_module(name, file, filename, description)
            return mod
        except ImportError, e:
            raise
        finally:
            file.close()

class Actor(object):
    def __init__(self, name, productName=None, configFile=None): 
        """ Build an Actor.

        Args:
            name         - the name of the actor: what name we are advertised as to tron.
            productName  - the name of the product; defaults to .name
            configFile   - the full path of the configuration file; defaults 
                            to $PRODUCTNAME_DIR/etc/$name.cfg
        """

        # Define/save the actor name, the product name, the product_DIR, and the
        # configuration file.
        self.name = name
        self.productName = productName if productName else self.name
        product_dir_name = '$%s_DIR' % (self.productName.upper())
        self.product_dir = os.path.expandvars(product_dir_name)
        
        if not self.product_dir:
            raise RuntimeError('environment variable %s must be defined' % (product_dir_name))

        self.configFile = configFile if configFile else \
            os.path.expandvars(os.path.join(self.product_dir, 'etc', '%s.cfg' % (self.name)))

        # Missing config bits should make us blow up.
        self.configFile = os.path.expandvars(self.configFile)
        logging.warn("reading config file %s", self.configFile)
        self.config = ConfigParser.ConfigParser()
        self.config.read(self.configFile)
        
        self.logDir = self.config.get(self.name, 'logdir')
        assert self.logDir, "logdir must be set!"

        # All loggers can and should be picked up by other modules with "logging.getLogger(name)"
        # Make the root logger go to a rotating file.
        opsLogging.makeOpsFileLogger(self.logDir, 'logs')
        self.logger = logging.getLogger('logs')
        self.logger.setLevel(int(self.config.get('logging','baseLevel')))
        self.logger.propagate = False
        self.logger.info('%s starting up....' % (name))

        opsLogging.makeOpsFileLogger(os.path.join(self.logDir, "cmds"), 'cmds')
        self.cmdlog = logging.getLogger('cmds')
        self.cmdlog.setLevel(int(self.config.get('logging','cmdLevel')))
        self.cmdlog.propagate = False

        self.console = logging.getLogger('') 
        try:
            self.console.setLevel(int(self.config.get('logging','consoleLevel')))
        except:
            self.console.setLevel(int(self.config.get('logging','baseLevel')))
 
        self.parser = CommandParser()

        # The list of all connected sources. 
        tronInterface = self.config.get('tron', 'interface') 
        tronPort = self.config.getint('tron', 'port') 
        self.commandSources = cmdLinkManager.listen(self, 
                                                    port=tronPort, 
                                                    interface=tronInterface) 
 
        # The Command which we send spontaneous output to. 
        self.bcast = actorCmd.Command(self.commandSources, 
                                      'self.0', 0, 0, None, immortal=True) 
 
        # commandSets are the command handler packages. Each handles 
        # a vocabulary, which it registers when loaded. 
        # We gather them in one place mainly so that "meta-commands" (init, status) 
        # can find the others. 
        self.commandSets = {}

        self.logger.info("Creating validation handler...")
        self.handler = validation.CommandHandler()
        
        self.logger.info("Attaching actorcore command sets...")
        self.attachAllCmdSets(path=os.path.join(os.path.expandvars('$ACTORCORE_DIR'),
                                                'python','actorcore','Commands'))
        self.logger.info("Attaching actor command sets...")
        self.attachAllCmdSets()
        self.logger.info("All command sets attached...")

        self.commandQueue = Queue.Queue()
        self.shuttingDown = False
        
    def attachCmdSet(self, cname, path=None):
        """ (Re-)load and attach a named set of commands. """

        if path == None:
            path = [os.path.join(self.product_dir, 'python', self.productName, 'Commands')]

        self.logger.info("attaching command set %s from path %s", cname, path)
               
        file = None
        try:
            file, filename, description = imp.find_module(cname, path)
            self.logger.debug("command set file=%s filename=%s from path %s",
                              file, filename, path)
            mod = imp.load_module(cname, file, filename, description)
        except ImportError, e:
            raise RuntimeError('Import of %s failed: %s' % (cname, e))
        finally:
            if file:
                file.close()

        # Instantiate and save a new command handler. 
        exec('cmdSet = mod.%s(self)' % (cname))

        # pdb.set_trace()
        
        # Check any new commands before finishing with the load. This
        # is a bit messy, as the commands might depend on a valid
        # keyword dictionary, which also comes from the module
        # file. So load the module's keys temporarily.
        if cmdSet.keys:
            keys.CmdKey.addKeys(cmdSet.keys)
        valCmds = []
        for v in cmdSet.vocab:
            try:
                verb, args, func = v
            except ValueError, e:
                raise RuntimeError("vocabulary word needs three parts: %s" % (v))

            # Check that the function exists and get its help.
            #
            funcDoc = inspect.getdoc(func)
            valCmd = validation.Cmd(verb, args, help=funcDoc) >> func
            valCmds.append(valCmd)

        #pdb.set_trace()
        # Got this far? Commit. Save the Cmds so that we can delete them later.
        oldCmdSet = self.commandSets.get(cname, None)
        cmdSet.validatedCmds = valCmds
        self.commandSets[cname] = cmdSet

        # Delete previous set of consumers for this named CmdSet, add new ones.
        if oldCmdSet:
            self.handler.removeConsumers(*oldCmdSet.validatedCmds)
        self.handler.addConsumers(*cmdSet.validatedCmds)

        self.logger.warn("handler verbs: %s" % (self.handler.consumers.keys()))
        
    def attachAllCmdSets(self, path=None):
        """ (Re-)load all command classes -- files in ./Command which end with Cmd.py.
        """

        if path == None:
            path = os.path.join(self.product_dir, 'python', self.productName, 'Commands')

        dirlist = os.listdir(path)
        dirlist.sort()

        for f in dirlist:
            if os.path.isdir(f) and not f.startswith('.'):
                self.attachAllCmdSets(path=f)
            if re.match('^[a-zA-Z][a-zA-Z0-9_-]*Cmd\.py$', f):
                self.attachCmdSet(f[:-3], [path])

    def actor_loop(self):
        """ Check the command queue and dispatch commands."""

        while True:
            try:
                # Get the next command
                self.logger.debug("Waiting for next command...")
                try:
                    cmd = self.commandQueue.get(block = True,timeout = 3)
                except Queue.Empty:
                    if self.shuttingDown:
                        return
                    else:
                        continue
                self.logger.info("Command received: %s" % (cmd))

                cmdStr = cmd.rawCmd

                try:
                    validatedCmd, cmdFuncs = self.handler.match(cmdStr)
                except Exception, e:
                    cmd.fail('text=%s' % (qstr("Unmatched command: %s (exception: %s)" %
                                               (cmdStr, e))))
                    tback('actor_loop', e)
                    continue

                if not validatedCmd:
                    cmd.fail('text=%s' % (qstr("Unrecognized command: %s (exception: %s)" %
                                               (cmdStr, e))))
                    continue
                
                self.logger.info('dispatching new command from %s:%d: %s' % (cmd.cmdr, cmd.mid, validatedCmd))
                self.activeCmd = cmd

                if len(cmdFuncs) > 1:
                    cmd.warn('text=%s' % (qstr("command has more than one callback (%s): %s" %
                                               (cmdFuncs, validatedCmd))))
                try:
                    cmd.cmd = validatedCmd
                    for func in cmdFuncs:
                        func(cmd)
                except Exception, e:
                    cmd.fail('text=%s' % (qstr("command failed: %s" % (e))))
                    tback('newCmd', e)
            except Exception, e:
                cmd.fail('text=%s' % (qstr("completely unexpected exception when processing a new command: %s" %
                                           (e))))
                try:
                    tback('newCmdFail', e)
                except:
                    pass
                
    def newCmd(self, cmd):
        """ Dispatch a newly received command. """

        self.activeCmd = None

        self.logger.info('new unqueued cmd: %s' % (cmd))
        
        # Empty cmds are OK; send an empty response... 
        if len(cmd.rawCmd) == 0:
            cmd.finish('')
            return None
        self.commandQueue.put(cmd)
        return self
    
    def _shutdown(self):
        self.shuttingDown = True
        
    def run(self, doReactor=True):
        logging.info("starting reactor....")
        try:
            threading.Thread(target=self.actor_loop).start()
            if doReactor:
                reactor.run()
        except Exception, e:
            tback('run', e)

        if doReactor:
            logging.info("reactor dead, cleaning up...")
            self._shutdown()
