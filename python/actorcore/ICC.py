from __future__ import print_function
from opscore.utility.qstr import qstr
import opscore.utility.sdss3logging as opsLogging
import logging

import imp
import os
import sys

import actorcore.Actor as coreActor

class ICC(coreActor.Actor):
    def __init__(self, name, **kwargs):
        coreActor.Actor.__init__(self, name, **kwargs)
        
        # Create a separate logger for controller io
        opsLogging.makeOpsFileLogger(os.path.join(self.logDir, "io"), 'io')
        self.iolog = logging.getLogger('io')
        self.iolog.setLevel(int(self.config.get('logging','ioLevel')))
        self.iolog.propagate = False

        self.controllers = dict()

    def attachController(self, name, instanceName=None, path=None, cmd=None):
        """ (Re-)load and attach a named set of commands. """

        if path is None:
            path = [os.path.join(self.product_dir, 'python', self.productName, 'Controllers')]

        if instanceName is None:
            instanceName = name
        self.logger.info("attaching controller %s/%s from path %s", instanceName, name, path)
        file = None
        try:
            file, filename, description = imp.find_module(name, path)
            self.logger.debug("controller file=%s filename=%s from path %s",
                              file, filename, path)
            mod = imp.load_module(name, file, filename, description)
            self.logger.debug('load_module(%s, %s, %s, %s) = %08x',
                              name, file, filename, description, id(mod))
        except ImportError as e:
            raise RuntimeError('Import of %s failed: %s' % (name, e))
        finally:
            if file:
                file.close()

        # Instantiate and save a new controller. 
        self.logger.info('creating new %s (%08x)', name, id(mod))
        try:
            controllerClass = getattr(mod, name)
        except AttributeError:
            self.logger.warn('text="controller module %s does not contain %s"',
                             name, name)
            return False
        
        try:
            conn = controllerClass(self, instanceName)
        except Exception as e:
            self.logger.warn('text="controller %s(%s) could not be created: %s"',
                             name, instanceName, e)
            return False
        
        # If we loaded the module and the controller is already running, cleanly stop the old one. 
        self.detachController(instanceName)

        self.logger.info('starting %s controller', instanceName)
        try:
            conn.start()
        except Exception as e:
            print(sys.exc_info())
            self.logger.error('Could not start controller %s/%s: %s', instanceName, name, e)
            return False
        
        self.controllers[instanceName] = conn
        return True

    def attachAllControllers(self, path=None):
        """ (Re-)load and (re-)connect to the hardware controllers listed in config:tron.controllers. 
        """

        for c in self.allControllers:
            if not self.attachController(c, path):
                self.bcast.warn('text="Could not connect to controller %s."' % (c))

    def detachController(self, controllerName):
        controller = self.controllers.get(controllerName)

        if controller:
            self.logger.info('stopping existing %s controller', controllerName)

            c = self.controllers[controllerName]
            del self.controllers[controllerName]

            c.stop()
        
    def stopAllControllers(self):
        for c in list(self.controllers.keys()):
            self.detachController(c)

    def shutdown(self):
        coreActor.shutdown(self)
        
        self.stopAllControllers()

