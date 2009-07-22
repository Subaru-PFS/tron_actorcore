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
from threading import Semaphore,Timer
from twisted.internet import reactor

from opscore.utility.qstr import qstr
from opscore.utility.tback import tback

import CommandLinkManager as cmdLinkManager
import Command as actorCmd

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
    def __init__(self, name, configFile, product_dir=None): 

        self.name = name
        self.configFile = os.path.expandvars(
            os.path.expanduser(configFile))

        if product_dir:
            self.product_dir = product_dir
        else:
            self.product_dir = os.environ.get('%s_DIR' % (name.upper()), None)

        if not self.product_dir:
            raise RuntimeError("either pass Actor(product_dir=XXX) or setup actorcore")
        
        # Missing config bits should make us blow up.
        self.configFile = os.path.expandvars(configFile)
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
        self.console.setLevel(int(self.config.get('logging','consoleLevel')))
 
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
        self.topCommands = {}
        self.logger.info("Attaching all command sets...")
        self.attachAllCommandSets()
        self.logger.info("All command sets attached...")

        self.commandQueue = Queue.Queue()
        self.shuttingDown = False
        
    def attachCmdSet(self, cname, path=None):
        """ (Re-)load and attach a named set of commands. """

        if path == None:
            path = [os.path.join(self.product_dir, 'python', self.name, 'Commands')]

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
        self.commandSets[cname] = cmdSet

        # Add the vocabulary. 
        for cmdName, cmdFunc in cmdSet.vocab.items():
            self.topCommands[cmdName] = cmdFunc

    def attachAllCommandSets(self, path=None):
        """ (Re-)load all command classes -- files in ./Command which end with Cmd.py.
        """

        if path == None:
            path = os.path.join(self.product_dir, 'python', self.name, 'Commands')

        dirlist = os.listdir(path)
        dirlist.sort()

        for f in dirlist:
            if os.path.isdir(f) and not f.startswith('.'):
                self.attachAllCommandSets(path=f)
            if re.match('^[a-zA-Z][a-zA-Z0-9_-]*Cmd\.py$', f):
                self.attachCmdSet(f[:-3], [path])

    def actor_loop(self):
        """ Check the command queue and dispatch commands."""
        while(1):
            try:
                cmd = self.commandQueue.get(block = True,timeout = 3)
            except Queue.Empty:
                if self.shuttingDown:
                    return
                else:
                    continue
            self.logger.info("Command received: %s" % (cmd))
            # Dispatch on the first word of the command. 
            handler = self.topCommands.get(cmd.cmd.name, None)
            if handler == None:
                cmd.fail('text="Unrecognized command: %s"' % (qstr(cmd.cmd.name, tquote="'")))
                return
            else:
                self.activeCmd = cmd
                try:
                    handler(cmd)
                except Exception, e:
                    tback('newCmd', e)
                    cmd.fail('text=%s' % (qstr("command failed: %s" % (e))))

    def newCmd(self, cmd):
        """ Dispatch a newly received command. """

        self.activeCmd = None

        self.logger.info('new cmd: %s' % (cmd))
        
        # Empty cmds are OK; send an empty response... 
        if len(cmd.cmd.name) == 0:
            cmd.finish('')
            return None
        self.commandQueue.put(cmd)
        return self
    
    def shutdown(self):
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
            self.shutdown()
