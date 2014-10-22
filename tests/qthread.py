import time
import unittest

import actorcore.QThread

class TestException(Exception):
    pass

class TestLogic(object):
    def __init__(self):
        self.stack = []

    def countMsg(self, n=1):
        return(range(n))

    def raiseMesg(self):
        raise TestException('kaboom')

    def pushMsg(self, item=None, pause=None):
        self.stack.append(item)
        if pause is not None:
            time.sleep(pause)
        
    def clearMsg(self):
        ret = self.stack
        self.stack = []

        return ret

class TestQThread(unittest.TestCase):
    def setUp(self):
        self.testLogic = TestLogic()
        self.testQ = actorcore.QThread.QThread(None, 'test_thread', timeout=2)
        self.testQ.startUp()

    def tearDown(self):
        self.testQ.exit()
        del self.testQ

    def test_call(self):
        n = 10
        ret = self.testQ.call(self.testLogic.countMsg, n=n)
        self.assertEqual(ret, range(n))

    def test_raise(self):
        with self.assertRaises(TestException):
            self.testQ.call(self.testLogic.raiseMesg)

    def test_thread1(self):
        n = 1000
        for i in range(n):
            self.testQ.putMsg(self.testLogic.pushMsg, item=i)
        ret1 = self.testQ.call(self.testLogic.clearMsg)
        self.assertEqual(ret1, range(n))

        for i in range(2):
            self.testQ.putMsg(self.testLogic.pushMsg, item=i)
        ret2 = self.testQ.call(self.testLogic.clearMsg)

        self.assertEqual(ret2, range(2))

    def test_urgent(self):
        for i in range(8):
            self.testQ.putMsg(self.testLogic.pushMsg, item=i, pause=0.25)

        # Force urgent readout of first 4 items.
        time.sleep(0.85)
        ret1 = self.testQ.call(self.testLogic.clearMsg, urgent=True)
        self.assertEqual(ret1, range(4))

        ret2 = self.testQ.call(self.testLogic.clearMsg)
        self.assertEqual(ret1+ret2, range(8))

def main(args=None):
    if isinstance(args, basestring):
        import shlex
        args = shlex.split(args)

    unittest.main(verbosity=9, failfast=True)

if __name__ == "__main__":
    main()
