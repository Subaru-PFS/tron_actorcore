#!/usr/bin/env python
"""A Model is a container for keyVars for an actor

History:
2009-03-30 ROwen
2009-07-18 ROwen    Modified to set the refreshCmd for each keyVar before adding it to the dispatcher.
2009-07-18 ROwen    Added keyVarDict property.
"""
import sys
import time
import traceback

import twisted.internet.reactor
import RO.Alg
import RO.Comm.HubConnection
import RO.Constants
import RO.StringUtil

from opscore.utility.twisted import cancelTimer
import opscore.protocols.keys as protoKeys
import opscore.protocols.parser as protoParse
import opscore.protocols.messages as protoMess
import keyvar

__all__ = ["Model"]

# number of keywork names to ask for in a given "keys getFor=actor" command
NumKeysToGetAtOnce = 20

class Model(object):
    """Subclass for your actor. Your subclass must use a singleton pattern
    because this class may only be instantiated once per actor.
    
    Before instantiating the first model, call setDispatcher (else you'll get a RuntimeError).
    
    Note: only keyVars defined in the actor's dictionary are refreshed automatically.
    Any keyVars you add to the subclass are synthetic keyVars that you should set yourself.
    """
    _registeredActors = set()
    dispatcher = None
    def __init__(self, actor):
        #print "%s.__init__(actor=%s)" % (self.__class__.__name__, actor)
        self._keyNameVarDict = dict()
        if actor in self._registeredActors:
            raise RuntimeError("%s model already instantiated" % (actor,))
        
        self.actor = actor
        if self.dispatcher == None:
            raise RuntimeError("Dispatcher not set")

        cachedKeyVars = []
        keysDict = protoKeys.KeysDictionary.load(actor)
        for key in keysDict.keys.itervalues():
            keyVar = keyvar.KeyVar(actor, key)
            if key.doCache and not keyVar.hasRefreshCmd:
                cachedKeyVars.append(keyVar)
            else:
                self.dispatcher.addKeyVar(keyVar)
            setattr(self, keyVar.name, keyVar)
        
        for ind in range(0, len(cachedKeyVars), NumKeysToGetAtOnce):
            keyVars = cachedKeyVars[ind:ind+NumKeysToGetAtOnce]
            keyNames = [(keyVar.name) for keyVar in keyVars]
            refreshCmdStr = "getFor=%s %s" % (self.actor, " ".join(keyNames))
            for keyVar in keyVars:
                keyVar.refreshActor = "keys"
                keyVar.refreshCmd = refreshCmdStr
                self.dispatcher.addKeyVar(keyVar)

        self._registeredActors.add(actor)
    
    @property
    def keyVarDict(self):
        """Return a dictionary of keyVar name:keyVar
        """
        retDict = dict()
        for name, item in self.__dict__.iteritems():
            if isinstance(item, keyvar.KeyVar):
                retDict[name] = item
        return retDict

    @classmethod
    def setDispatcher(cls, dispatcher):
        #print "%s.setDispatcher(dispatcher=%s)" % (cls.__name__, dispatcher)
        if cls.dispatcher:
            raise RuntimeError("Dispatcher cannot be modified once set")
        cls.dispatcher = dispatcher
