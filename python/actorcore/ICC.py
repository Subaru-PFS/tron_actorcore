import logging
import os
from importlib import reload
from importlib.util import find_spec, spec_from_file_location

import actorcore.Actor as coreActor
import opscore.utility.sdss3logging as opsLogging
from opscore.utility.qstr import qstr


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

        if instanceName is None:
            instanceName = name

        if cmd is None:
            cmd = self.bcast

        if path is not None:
            cmd.inform(f'text="attaching controller {instanceName}/{name} from {path}"')
            spec = spec_from_file_location(name, location=path)

        else:
            cmd.inform(f'text="attaching controller {instanceName} from {self.productName}.Controllers.{name}"')
            spec = find_spec(f'{self.productName}.Controllers.{name}')

        try:
            if spec is None:
                # the module might be nested in Controllers.__init__py
                cmd.warn('text="module not found.. checking for nested imports..."')
                spec = find_spec(f'{self.productName}.Controllers')
                cmd.inform(f'text={qstr(spec)}')
                # loading controllers module  and try to retrieve the required module as an attribute.
                controllers = spec.loader.load_module()
                mod = getattr(controllers, name)
            else:
                cmd.inform(f'text={qstr(spec)}')
                mod = spec.loader.load_module()

        except Exception as e:
            raise RuntimeError('Import of %s failed: %s' % (name, e))

        # just reloading by safety
        mod = reload(mod)
        cmd.inform(f'text={qstr(mod)}')

        # Instantiate and save a new controller.
        cmd.inform('text="creating new %s (%08x)"' % (name, id(mod)))
        try:
            controllerClass = getattr(mod, name)
        except AttributeError:
            cmd.warn(f'text="controller module {name} does not contain {name}"')
            raise

        try:
            conn = controllerClass(self, instanceName)
        except:
            cmd.warn(f'text="controller {name}({instanceName}) could not be created"')
            raise

        # If we loaded the module and the controller is already running, cleanly stop the old one.
        self.detachController(instanceName)

        cmd.inform(f'text="starting {instanceName} controller"')

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
