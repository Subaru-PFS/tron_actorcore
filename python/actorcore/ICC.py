import imp
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
import actorcore.CommandLinkManager as cmdLinkManager
import actorcore.Command as actorCmd
import actorcore.ICC as actorICC

from ICCExceptions import ICCError
from STimer import STimer


class ICC(object):
    def __init__(self, name, configFile): 
        self.name = name
        self.configFile = os.path.expandvars(
            os.path.expanduser(configFile)))

        # Missing config bits should make us blow up.
        self.configFile = os.path.expandvars(configFile)
        logging.warn("reading config file %s", self.configFile)
        self.config = ConfigParser.ConfigParser()
        self.config.read(self.configFile)
        
        self.logDir = self.config.get(self.name, 'logdir')
        assert self.logDir, "logdir must be set!"

        # All loggers can and should be picked up by other modules with "logging.getLogger(name)"
        # Make the root logger go to a rotating file.
        opsLogging.makeOpsFileLogger(os.path.join(self.logDir, "icc"), 'icc')
        self.icclog = logging.getLogger('icc')
        self.icclog.setLevel(int(self.config.get('logging','iccLevel')))
        self.icclog.propagate = False
        self.icclog.info('ICC starting up....')

        # Create separate loggers for controller io and for commands
        opsLogging.makeOpsFileLogger(os.path.join(self.logDir, "io"), 'io')
        self.iolog = logging.getLogger('io')
        self.iolog.setLevel(int(self.config.get('logging','ioLevel')))
        self.iolog.propagate = False

        opsLogging.makeOpsFileLogger(os.path.join(self.logDir, "cmds"), 'cmds')
        self.cmdlog = logging.getLogger('cmds')
        self.cmdlog.setLevel(int(self.config.get('logging','cmdLevel')))
        self.cmdlog.propagate = False

        console = logging.getLogger('')                                                                                                       
        console.setLevel(int(self.config.get('logging','logging')))
        logging.info("reading config file %s", self.configFile) 
                                                                                                                                              
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
        logging.info("Attaching all command sets...")
        self.attachAllCommandSets()
        logging.info("All command sets attached...")

        self.commandQueue = Queue.Queue()

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

   def attachCmdSet(self, cname, path=None):
        """ (Re-)load and attach a named set of commands.                                                                                     
                                                                                                                                              
        """

        if path == None:
            path = ['./Commands']

        self.icclog.info("attaching command set %s from path %s", cname, path)

        file = None
        try:
            file, filename, description = imp.find_module(cname, path)
            self.icclog.debug("command set file=%s filename=%s from path %s",
                              file, filename, path)
            mod = imp.load_module(cname, file, filename, description)
        except ImportError, e:
            raise ICCError('Import of %s failed: %s' % (cname, e))
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
        """ (Re-)load all command classes.                                                                                                    
        """

        if path == None:
            path = './Commands'
        dirlist = os.listdir(path)
        dirlist.sort()

        for f in dirlist:
            if os.path.isdir(f) and not f.startswith('.'):
                self.attachAllCommandSets(path=f)
            if f.endswith('Cmd.py'):
                self.attachCmdSet(f[:-3], [path])


   def attachController(self, name, path=None, cmd=None):
        """ (Re-)load and attach a named set of commands. """

        if path == None:
            path = ['./Controllers']

        logging.info("attaching controller %s from path %s", name, path)
        file = None
        try:
            file, filename, description = imp.find_module(name, path)
            self.icclog.debug("controller file=%s filename=%s from path %s",
                              file, filename, path)
            mod = imp.load_module(name, file, filename, description)
            logging.debug('load_module(%s, %s, %s, %s) = %08x',
                         name, file, filename, description, id(mod))
        except ImportError, e:
            raise ICCError('Import of %s failed: %s' % (name, e))
        finally:
            if file:
                file.close()

        # Instantiate and save a new controller.                                                                                              
        logging.info('creating new %s (%08x)', name, id(mod))
        exec('conn = mod.%s(self, "%s")' % (name, name))

        # If we loaded the module and the controller is already running, cleanly stop the old one.                                            
        if name in self.controllers:
            self.icclog.info('stopping %s controller', name)
            self.controllers[name].stop()
            del self.controllers[name]

        logging.info('starting %s controller', name)
        try:
            conn.start()
        except ICCError, e:
            print sys.exc_info()
            logging.error('Could not connect to %s', name)
            return False
        self.controllers[name] = conn
        return True

    def attachAllControllers(self, path=None):
        """ (Re-)load and (re-)connect to the hardware controllers listed in config:tron.controllers.                                         
        """

	clist = eval(self.config.get(self.name, 'controllers'))
        logging.info("All controllers = %s",clist)
        for c in clist:
            if c not in self.allControllers:
                self.bcast.warn('text=%s' % (qstr('cannot attach unknown controller %s' % (c))))
                continue
            if not self.attachController(c, path):
                self.bcast.warn('text="Could not connect to controller %s."' % (c))

    def stopAllControllers(self):
        for c in self.controllers.keys():
            controller = self.controllers[c]
            controller.stop()

   def icc_loop(self):

        """ Check the command queue and dispatch commands."""
        while(1):
            try:
                cmd = self.command_queue.get(block = True,timeout = 3)
            except Queue.Empty:
                if self.shutdown:
                    return
            logging.info("Command received.")
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

        # Empty cmds are OK; send an empty response...                                                                                        

        if len(cmd.cmd.name) == 0:
            cmd.finish('')
            return None
        self.command_queue.put(cmd)
        return self

    def run(self):
        # Surely I need to wrap this in a try...except...                                                                                     
        logging.info("starting reactor....")
        try:
            threading.Thread(target=self.icc_loop).start()
            reactor.run()
        except Exception, e:
            tback('run', e)
            
        logging.info("reactor dead, cleaning up...")
        self.shutdown = True
        self.stopAllControllers()
