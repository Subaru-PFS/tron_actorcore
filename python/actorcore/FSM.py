import logging

import fysom


class States(fysom.Fysom):
    def __init__(self, stateChangeCB):
        fysom.Fysom.__init__(self, {'initial': 'none',
                                    'events': [{'name': 'start', 'src': 'none', 'dst': 'OFF'},
                                               {'name': 'toLoaded', 'src': ['OFF', 'ONLINE'], 'dst': 'LOADED'},
                                               {'name': 'toOnline', 'src': 'LOADED', 'dst': 'ONLINE'},
                                               {'name': 'stop', 'src': ['OFF',' LOADED', 'ONLINE'], 'dst': 'OFF'}],
                                    })
        self.onOFF = stateChangeCB


class Substates(fysom.Fysom):
    def __init__(self, topState, substates, events, stateChangeCB):
        self.topState = topState
        self.dictState = {'OFF': ['IDLE', 'LOADING', 'FAILED'],
                          'LOADED': ['IDLE', 'INITIALISING', 'FAILED'],
                          'ONLINE': substates}

        events += [{'name': 'start', 'src': 'none', 'dst': 'IDLE'},
                   {'name': 'load', 'src': 'IDLE', 'dst': 'LOADING'},
                   {'name': 'init', 'src': 'IDLE', 'dst': 'INITIALISING'},
                   {'name': 'idle', 'src': ['LOADING', 'INITIALISING'], 'dst': 'IDLE'},
                   {'name': 'fail', 'src': ['LOADING', 'INITIALISING'], 'dst': 'FAILED'}]

        fysom.Fysom.__init__(self, {'initial': 'none', 'events': events})

        for state in substates:
            setattr(self, 'on%s' % state.upper(), stateChangeCB)

        for event in events:
            setattr(self, 'onbefore%s' % event['name'], self.checkTransition)

    def checkTransition(self, e):
        if e.dst not in self.dictState[self.topState.current]:
            raise fysom.FysomError('FysomError: event %s inappropriate in top state %s' % (e.event,
                                                                                           self.topState.current))


class FSMDev(object):
    def __init__(self, actor, name, events=False, substates=False, loglevel=logging.DEBUG):
        # This sets up the connections to/from the hub, the logger, and the twisted reactor.
        #

        self.actor = actor
        self.name = name
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(loglevel)
        events = [] if not events else events
        substates = ['IDLE', 'FAILED'] if not substates else substates

        self.states = States(stateChangeCB=self.statesCB)

        self.substates = Substates(topState=self.states,
                                   substates=substates,
                                   events=events,
                                   stateChangeCB=self.statesCB)

        self.addStateCB('LOADING', self.loadDevice)
        self.addStateCB('INITIALISING', self.initDevice)

        self.states.start()

    def loadDevice(self, e):
        try:
            self.loadCfg(cmd=e.cmd, mode=e.mode)
            self.startComm(cmd=e.cmd)

            self.states.toLoaded()
            self.substates.idle(cmd=e.cmd)
        except:
            self.substates.fail(cmd=e.cmd)
            raise

    def initDevice(self, e):
        try:
            self.init(cmd=e.cmd)

            self.states.toOnline()
            self.substates.idle(cmd=e.cmd)
        except:
            self.substates.fail(cmd=e.cmd)
            raise

    def addStateCB(self, state, callback):
        def func(obj, *args, **kwargs):
            self.statesCB(obj)
            return callback(obj, *args, **kwargs)

        setattr(self.substates, 'on%s' % state, func)

    def start(self, cmd=None, doInit=False, mode=None):
        # start load event which will trigger loadDevice Callback
        cmd = self.actor.bcast if cmd is None else cmd

        self.substates.start(cmd=cmd)
        self.substates.load(cmd=cmd, mode=mode)

        # Trigger initDevice Callback if init is set automatically
        if doInit:
            self.substates.init(cmd=cmd)

    def stop(self, cmd=None):
        cmd = self.actor.bcast if cmd is None else cmd
        self.states.stop(cmd=cmd)

    def statesCB(self, e):
        try:
            cmd = e.cmd
        except AttributeError:
            cmd = self.actor.bcast

        self.updateStates(cmd=cmd)

    def updateStates(self, cmd):
        cmd.inform('%sFSM=%s,%s' % (self.name, self.states.current, self.substates.current))

        # Update actor state and substate, 'logical and' of lower controllers state make sense

        try:
            self.actor.updateStates(cmd=cmd, onsubstate=self.substates.current)
        except Exception as e:
            cmd.warn('text=%s' % self.actor.strTraceback(e))

    def loadCfg(self, cmd, mode=None):
        cmd.inform("text='Config Loaded'")

    def startComm(self, cmd):
        cmd.inform("text='Communication established with controller'")

    def init(self, cmd):
        cmd.inform("text='Init Device OK'")
