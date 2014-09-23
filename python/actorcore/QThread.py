import functools
import Queue
import threading
from opscore.utility.tback import tback

"""
Usage:

Given a QThread found with, say:
   tq = self.actor.devices[deviceName]

Asynchronous calls:

   tq.callMsgLater(10.0, tq.pingMsg, cmd=cmd)

 or:

   tq = self.actor.devices[deviceName]
   ret = tq.call(tq.statusMsg, cmd=cmd)


   msg = QMsg(tq.pingMsg, cmd=cmd)
   tq.queue.put(msg)

 or:

"""

class QMsg(object):
    DEFAULT_PRIORITY = 5

    def __init__(self, method, returnQueue=None, msgPriority=None, **argd):
        """ Create a new QMsg, which will resolve to method(*argl, **argd). 

        d = {}
        qm = QMsg(d.__setitem__, 'abc', 42)

        qt = QThread(....)
        qm = QMsg(qt.pingMsg, cmd)
        
        """

        self.priority = msgPriority if msgPriority else QMsg.DEFAULT_PRIORITY
        self.returnQueue = returnQueue
        self.method = functools.partial(method, **argd)

    def __lt__(self, other):
        """ Support sorting of QMsg instances, based on the .priority. Used by PriorityQueue. """

        return self.priority < other.priority

class QThread(threading.Thread):
    def __init__(self, actor, name, timeout=10.0, isDaemon=True, queueClass=Queue.PriorityQueue):
        """ A thread with a queue. The thread's .run() method pops items off its public .queue and executes them. """

        threading.Thread.__init__(self, name=name)
        self.setDaemon(isDaemon)
        if actor is None:
            import FakeActor
            actor = FakeActor.FakeActor()
        self.actor = actor
        self.timeout = timeout
        self.exitASAP = False

        self.queue = queueClass()

    def _realCmd(self, cmd=None):
        """ Returns a callable cmd instance. If the passed in cmd is None, return the actor's bcast cmd. """

        return cmd if cmd else self.actor.bcast

    def putMsg(self, method, **argd):
        """ send ourself a new message. 

        Args:
            method: a function or bound method to call
            **argd: the arguments to the method.
        """

        qmsg = QMsg(method, **argd)
        self.queue.put(qmsg)
        
    def call(self, method, callTimeout=None, **argd):
        """ send ourself a new message, then wait for and return a single response. 

        Arguments
        ---------
        method: a function or bound method to call
        **argd" the arguments to the method.

        We steal argd['callTimeout'], if found.
        We add argd['__returnQueue'], and require that it does not exist.

        Notes
        -----
        It is entirely up to the called method to behave correctly and to always .put()
        a single element on the queue.

        Throws
        ------
        RuntimeError if out mechanism has name conflicts with argd.
        """
        
        if threading.currentThread() == self:
            raise RuntimeError("cannot .call() a QThread from its own thread of control.")

        returnQueue = Queue.Queue()
        
        qmsg = QMsg(method, returnQueue=returnQueue, **argd)
        self.queue.put(qmsg)

        try:
            ret = returnQueue.get(timeout=callTimeout)
        except Queue.Empty:
            # Annotate with _something_ informative.
            raise Queue.Empty("empty return from %s in %s" % (qmsg, self))

        if isinstance(ret, Exception):
            raise ret
        else:
            return ret

    def sendLater(self, msg, deltaTime, priority=1):
        """ Send ourself a QMsg after deltaTime seconds. """

        def _push(queue=self.queue, msg=msg):
            queue.put(msg)
            
        t = threading.Timer(deltaTime, _push)
        t.start()

        return t
        
    def handleTimeout(self):
        """ Called when the .get() times out. Intended to be overridden. """

        self._realCmd(None).diag('text="%s thead is alive (exiting=%s)"' % (self.name, self.exitASAP))
        if self.exitASAP:
            raise SystemExit()


    def exit(self):
        """ Signal our thread in .run() that it should exit. """

        self.exitASAP = True
        self.putMsg(self.exitMsg)

    def exitMsg(self, cmd=None):
        """ handler for the "exit" message. Spits out a message and arranges for the .run() method to exit.  """

        print('in %s exitMsg' % (self.name))
        raise SystemExit()

    def pingMsg(self, cmd=None):
        """ handler for the 'ping' message. """

        self._realCmd(cmd).inform('text="thread %s is alive!"' % (self.name))

    
    def run(self):
        """ Main run loop for this thread. """

        self._realCmd(None).diag("%s thread has started .run()" % (self.name))
        while True:
            try:
                msg = self.queue.get(timeout=self.timeout)
                    
                qlen = self.queue.qsize()
                if qlen > 0:
                    self._realCmd(None).diag("%s thread has %d items after a .get()" % (self.name, qlen))

                # I envision accepting other types .
                if isinstance(msg, QMsg):
                    method = msg.method
                    returnQueue = msg.returnQueue
                else:
                    raise AttributeError("thread %s received a message of an unhanded type(s): %s" % 
                                         (self.name, type(msg), msg))
                ret = None
                try:
                    ret = method()
                except SystemExit:
                    return
                except Exception as e:
                    self._realCmd(None).warn('text="%s: uncaught exception running %s: %s"' % 
                                             (self.name, method, e))
                    ret = e
                finally:
                    if returnQueue:
                        returnQueue.put(ret)
                    self._realCmd(None).diag("returnQueue=%s ret=%s" % (returnQueue, ret))

            except Queue.Empty:
                self.handleTimeout()
            except Exception, e:
                try:
                    emsg = 'text="%s thread got unexpected exception: %s"' % (self.name, e)
                    self._realCmd().diag(emsg)
                    tback("DeviceThread", e)
                except:
                    print emsg
                    tback("DeviceThread", e)
