from opscore.utility.qstr import qstr
import opscore.utility.sdss3logging as opsLogging
import logging

import imp
import os
import sys
import threading

import actorcore.Actor as coreActor

class ICC(coreActor.Actor):
    def __init__(self, name, configFile, productName=None):
        coreActor.Actor.__init__(self, name, configFile=configFile, productName=productName)
        
        # Create a separate logger for controller io
        opsLogging.makeOpsFileLogger(os.path.join(self.logDir, "io"), 'io')
        self.iolog = logging.getLogger('io')
        self.iolog.setLevel(int(self.config.get('logging','ioLevel')))
        self.iolog.propagate = False

    def attachController(self, name, path=None, cmd=None):
        """ (Re-)load and attach a named set of commands. """

        if path == None:
            path = ['./Controllers']

        # import pdb; pdb.set_trace()
        self.logger.info("attaching controller %s from path %s", name, path)
        file = None
        try:
            file, filename, description = imp.find_module(name, path)
            self.logger.debug("controller file=%s filename=%s from path %s",
                              file, filename, path)
            mod = imp.load_module(name, file, filename, description)
            self.logger.debug('load_module(%s, %s, %s, %s) = %08x',
                         name, file, filename, description, id(mod))
        except ImportError, e:
            raise RuntimeError('Import of %s failed: %s' % (name, e))
        finally:
            if file:
                file.close()

        # Instantiate and save a new controller. 
        self.logger.info('creating new %s (%08x)', name, id(mod))
        exec('conn = mod.%s(self, "%s")' % (name, name))

        # If we loaded the module and the controller is already running, cleanly stop the old one. 
        if name in self.controllers:
            self.logger.info('stopping %s controller', name)
            self.controllers[name].stop()
            del self.controllers[name]

        self.logger.info('starting %s controller', name)
        try:
            conn.start()
        except Exception, e:
            print sys.exc_info()
            self.logger.error('Could not connect to %s', name)
            return False
        self.controllers[name] = conn
        return True

    def attachAllControllers(self, path=None):
        """ (Re-)load and (re-)connect to the hardware controllers listed in config:tron.controllers. 
        """

	clist = eval(self.config.get(self.name, 'controllers'))
        self.logger.info("All controllers = %s",clist)
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

    def shutdown(self):
        actorCore.Actor.shutdown(self)
        
        self.stopAllControllers()

