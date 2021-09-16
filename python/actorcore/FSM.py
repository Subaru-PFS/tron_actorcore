import fysom
from functools import partial
from twisted.internet import reactor


class MetaStates(object):
    statesLogic = dict(OFF=2, LOADED=1, ONLINE=0)
    inactives = dict(FAILED=100, IDLE=0)

    def __init__(self, FSMActor):
        self.actor = FSMActor
        self.actives = dict()

    @property
    def controllers(self):
        return list(self.actor.controllers.values())

    @property
    def states(self):
        """Return list of controller current state."""
        return list(set([controller.states.current for controller in self.controllers]))

    @property
    def substates(self):
        """Return list of controller current substate."""
        return list(set([controller.substates.current for controller in self.controllers]))

    @property
    def state(self):
        """Return current meta state as a result of underlying state machine."""
        if not self.controllers:
            return 'OFF'

        return self.maxLogicalState(self.states, logicDict=MetaStates.statesLogic)

    @property
    def substate(self):
        """Return current meta substate as a result of underlying state machine."""
        if not self.controllers:
            return 'IDLE'

        allLogic = dict([kv for kv in MetaStates.inactives.items()] + [kv for kv in self.actives.items()])
        return self.maxLogicalState(self.substates, logicDict=allLogic)

    def maxLogicalState(self, stateList, logicDict):
        """Return current enu meta substate as a result of underlying state machine."""
        statesLogic = [(state, logicDict[state]) for state in stateList]
        state, logicNumber = max(statesLogic, key=lambda item: item[1])
        return state

    def update(self, cmd, current=False):
        """Generate metaFSM keyword."""
        self.clearObsolete()
        if current and current not in self.inactives.keys():
            self.actives[current] = self.generateActiveLogicNumber()

        cmd.inform('metaFSM=%s,%s' % (self.state, self.substate))

    def clearObsolete(self):
        """Clear obsolete active substates."""
        for substate in list(set(self.actives.keys()) - set(self.substates)):
            self.actives.pop(substate, None)

    def generateActiveLogicNumber(self):
        """Generate active logic number, more recent wins."""
        if not self.actives.values():
            return 1

        return max(self.actives.values()) + 1


class States(fysom.Fysom):
    def __init__(self, stateChangeCB):
        fysom.Fysom.__init__(self, {'initial': 'none',
                                    'events': [{'name': 'start', 'src': 'none', 'dst': 'OFF'},
                                               {'name': 'toLoaded', 'src': ['OFF', 'ONLINE'], 'dst': 'LOADED'},
                                               {'name': 'toOnline', 'src': 'LOADED', 'dst': 'ONLINE'},
                                               {'name': 'stop', 'src': ['OFF', 'LOADED', 'ONLINE'], 'dst': 'OFF'}],
                                    })
        self.onOFF = stateChangeCB


class Substates(fysom.Fysom):
    def __init__(self, topstate, substates, events, stateChangeCB):
        self.topstate = topstate
        self.dictState = {'OFF': ['IDLE', 'LOADING', 'FAILED'],
                          'LOADED': ['IDLE', 'INITIALISING', 'SAFESTOP', 'FAILED'],
                          'ONLINE': substates}

        events += [{'name': 'start', 'src': 'none', 'dst': 'IDLE'},
                   {'name': 'load', 'src': 'IDLE', 'dst': 'LOADING'},
                   {'name': 'init', 'src': 'IDLE', 'dst': 'INITIALISING'},
                   {'name': 'idle', 'src': ['LOADING', 'INITIALISING', 'SAFESTOP'], 'dst': 'IDLE'},
                   {'name': 'safestop', 'src': 'IDLE', 'dst': 'SAFESTOP'},
                   {'name': 'fail', 'src': ['LOADING', 'INITIALISING'], 'dst': 'FAILED'},
                   {'name': 'acknowledge', 'src': 'FAILED', 'dst': 'IDLE'}]

        fysom.Fysom.__init__(self, {'initial': 'none', 'events': events})

        for state in substates:
            setattr(self, 'on%s' % state.upper(), stateChangeCB)

        for event in events:
            setattr(self, 'onbefore%s' % event['name'], self.checkTransition)

    def checkTransition(self, event):
        if event.dst not in self.dictState[self.topstate.current]:
            raise fysom.FysomError('FysomError: event %s inappropriate in top state %s' % (event.event,
                                                                                           self.topstate.current))


class FSMDevice(object):
    ignore = ['fsm', 'event', 'src', 'dst', 'args']

    def __init__(self, actor, name, events=False, substates=False):
        # This sets up the connections to/from the hub, the logger, and the twisted reactor.

        self.actor = actor
        self.name = name

        events = [] if not events else events
        substates = ['IDLE', 'FAILED'] if not substates else substates

        self.states = States(stateChangeCB=self.statesCB)

        self.substates = Substates(topstate=self.states,
                                   substates=substates,
                                   events=events,
                                   stateChangeCB=self.statesCB)

        self.addStateCB('LOADING', self.loading)
        self.addStateCB('INITIALISING', self.initialising)

        self.states.start()

    def loading(self, cmd, mode=None):
        self.loadCfg(cmd, mode=mode)
        self.openComm(cmd)
        self.testComm(cmd)
        self.states.toLoaded()

    def initialising(self, cmd, **kwargs):
        self.init(cmd, **kwargs)
        self.states.toOnline()

    def addStateCB(self, state, callback):
        def func(event):
            self.statesCB(event)
            cmd = event.args[0] if len(event.args) else self.actor.bcast
            kwargs = dict([(key, val) for key, val in event.__dict__.items() if key not in FSMDevice.ignore])

            try:
                ret = callback(*event.args, **kwargs)
            except UserWarning:
                self.substates.idle()
                raise
            except:
                self.substates.fail()
                raise

            self.substates.idle()

        setattr(self.substates, 'on%s' % state, func)

    def start(self, cmd=None, doInit=False, mode=None):
        # start load event which will trigger loadDevice Callback
        cmd = self.actor.bcast if cmd is None else cmd

        self.substates.start(cmd)
        self.substates.load(cmd, mode=mode)

        # Trigger initDevice Callback if init is set automatically
        if doInit:
            self.substates.init(cmd)

    def stop(self, cmd=None):
        cmd = self.actor.bcast if cmd is None else cmd
        self.states.stop(cmd)

        reactor.callLater(2, partial(self.actor.callCommand, 'status'))

    def statesCB(self, event):
        cmd = event.args[0] if len(event.args) else self.actor.bcast
        self.updateStates(cmd=cmd)

    def updateStates(self, cmd):
        cmd.inform('%sFSM=%s,%s' % (self.name, self.states.current, self.substates.current))
        # Update actor state and substate, 'logical and' of lower controllers state make sense

        try:
            self.actor.metaStates.update(cmd=cmd, current=self.substates.current)
        except Exception as e:
            cmd.warn('text=%s' % self.actor.strTraceback(e))

    def loadCfg(self, cmd, mode=None):
        cmd.inform("text='%s configuration correctly Loaded'" % self.name)

    def openComm(self, cmd):
        cmd.inform("text='%s communication is open'" % self.name)

    def testComm(self, cmd):
        cmd.inform("text='%s communication is functional'" % self.name)

    def init(self, cmd, *args, **kwargs):
        cmd.inform("text='%s initialisation OK'" % self.name)
