# import our routines before logging itself.
from builtins import object
try:
    import opscore.utility.sdss3logging as opsLogging
except:
    pass

import logging

class FakeCommand(object):
    def __init__(self, logger=None):
        self.logger = logger if logger else logging.getLogger()

    def respond(self, s):
        self.logger.info("fake.inform: %s" % (s))
    def inform(self, s):
        self.logger.info("fake.inform: %s" % (s))
    def diag(self, s):
        self.logger.debug("fake.diag: %s" % (s))
    def warn(self, s):
        self.logger.warn("fake.warn: %s" % (s))
    def fail(self, s):
        self.logger.error("fake.fail: %s" % (s))
    def finish(self, s):
        self.logger.info("fake.finish: %s" % (s))

class FakeActor(object):
    """ A minimal object to stand in for a proper Actor. """

    def __init__(self):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug("FakeActor starting up")
        self.models = {}
        self.bcast = FakeCommand(self.logger)
        self.cmdr = None

        self.bcast.inform("FakeActor started")
