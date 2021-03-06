from __future__ import print_function

import imp
import logging
import os

import actorcore.Actor as coreActor
import opscore.utility.sdss3logging as opsLogging


class ICC(coreActor.Actor):
    def __init__(self, name, **kwargs):
        coreActor.Actor.__init__(self, name, **kwargs)

        # Create a separate logger for controller io
        opsLogging.makeOpsFileLogger(os.path.join(self.logDir, "io"), 'io')
        self.iolog = logging.getLogger('io')
        self.iolog.setLevel(int(self.config.get('logging', 'ioLevel')))
        self.iolog.propagate = False

        self.controllers = dict()

    def attachController(self, name, instanceName=None, path=None, cmd=None, **kwargs):
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
            self.logger.warn('text="controller module %s does not contain %s"', name, name)
            raise

        try:
            conn = controllerClass(self, instanceName)
        except:
            self.logger.warn('text="controller %s(%s) could not be created"', name, instanceName)
            raise
        # If we loaded the module and the controller is already running, cleanly stop the old one.
        self.detachController(instanceName)

        self.logger.info('starting %s controller', instanceName)

        self.controllers[instanceName] = conn
        conn.start(cmd=cmd, **kwargs)

    def attachAllControllers(self, path=None):
        """ (Re-)load and (re-)connect to the hardware controllers listed in config:tron.controllers.
        """

        for c in self.allControllers:
            try:
                self.attachController(c, path)
                self.callCommand('%s status' % c)

            except Exception as e:
                self.logger.warn('text=%s' % self.strTraceback(e))

    def detachController(self, controllerName, cmd=None):
        cmd = self.bcast if cmd is None else cmd
        controller = self.controllers.get(controllerName)

        if controller:
            self.logger.info('stopping existing %s controller', controllerName)
            try:
                controller.stop(cmd=cmd)
            except Exception as e:
                cmd.warn('text=%s' % self.strTraceback(e))

            del self.controllers[controllerName]

    def stopAllControllers(self):
        for c in list(self.controllers.keys()):
            self.detachController(c)

    def shutdown(self):
        coreActor.shutdown(self)

        self.stopAllControllers()
