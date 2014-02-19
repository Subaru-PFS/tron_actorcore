# import our routines before logging itself. 
try:
    import opscore.utility.sdss3logging as opsLogging
except:
    pass

import logging

class FakeCommand(object):
    def inform(self, s):
        print("fake.inform: %s" % (s))
    def diag(self, s):
        print("fake.diag: %s" % (s))
    def warn(self, s):
        print("fake.warn: %s" % (s))
    def fail(self, s):
        print("fake.fail: %s" % (s))
    def finish(self, s):
        print("fake.finish: %s" % (s))

class FakeActor(object):
    """ A minimal object to stand in for a proper Actor. """

    def __init__(self):
        self.models = {}
        self.bcast = FakeCommand()
        self.cmdr = None
