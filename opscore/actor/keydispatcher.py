#!/usr/bin/env python
"""Sets KeyVars based on replies from the hub.

History:
2009-04-03 ROwen    Split out basic functionality from full dispatcher.
2009-07-18 ROwen    Added __all__.
2009-07-23 ROwen    Added doCallbacks argument to dispatchReply and dispatchReplyStr
                    to support delayCallbacks in CmdKeyVarDispatcher.
"""
import sys
import traceback
import opscore.protocols.parser
import keyvar

__all__ = ["KeyVarDispatcher"]

class KeyVarDispatcher(object):
    """Parse replies and set KeyVars.
    """
    def __init__(self):
        """Create a new KeyVarDispatcher
        """
        self.parser = opscore.protocols.parser.ReplyParser()

        # dictionary of lists of KeyVars; keys are (actor.lower(), keyName.lower()) tuples;
        # values are lists of KeyVars
        # (having a list of KeyVars allows more than one KeyVar for the same actor keyword)
        self.keyVarListDict = dict()

        # set of actors for which loadActorDictionary has been called
        self.loadedActors = set()
    
    def addKeyVar(self, keyVar):
        """
        Adds a keyword variable (opscore.actor.keyvar.KeyVar) to the collection.
        
        Inputs:
        - keyVar: the keyword variable (opscore.actor.keyvar.KeyVar)
        """
        dictKey = self._makeDictKey(keyVar.actor, keyVar.name)
        # get list of existing keyVars with the same actor and keyword name,
        # adding an empty list to self.keyVarListDict if none are already present
        keyList = self.keyVarListDict.setdefault(dictKey, [])
        # append new keyVar to the list
        keyList.append(keyVar)

    def dispatchReply(self, reply, doCallbacks=True):
        """Set KeyVars based on the supplied Reply
        
        reply is a parsed Reply object (opscore.protocols.messages.Reply) whose fields include:
         - header.program: name of the program that triggered the message (string)
         - header.commandId: command ID that triggered the message (int) 
         - header.actor: the actor that generated the message (string)
         - header.code: the message type code (opscore.protocols.types.Enum)
         - string: the original unparsed message (string)
         - keywords: an ordered dictionary of message keywords (opscore.protocols.messages.Keywords)        
        Refer to https://trac.sdss3.org/wiki/Ops/Protocols for details.
        """
#         print "dispatchReply(reply=%s, doCallbacks=%s)" % (reply, doCallbacks)
        actor = reply.header.actor.lower()
        if actor.startswith("keys_"):
            # data is from the hub's keyword cache
            actor = actor[5:]
            isGenuine = False
        else:
            isGenuine = True
        for keyword in reply.keywords:
            keyVarList = self.getKeyVarList(actor, keyword.name)
            for keyVar in keyVarList:
                try:
                    keyVar.set(keyword.values, isGenuine=isGenuine, reply=reply, doCallbacks=doCallbacks)
                except:
                    traceback.print_exc(file=sys.stderr)
                    
    def dispatchReplyStr(self, replyStr, doCallbacks=True):
        """Read, parse and dispatch a message from the hub.
        """
#        print "%s.dispatchReplyStr(%r)" % (self.__class__.__name__, replyStr)
        reply = self.parser.parse(replyStr)
        self.dispatchReply(reply, doCallbacks=doCallbacks)

    def getKeyVarList(self, actor, keyName):
        """Return the list of KeyVars by this name and actor; return [] if no match.
        
        Do not modify the returned list. It may not be a copy.
        """
        return self.keyVarListDict.get(self._makeDictKey(actor, keyName), [])

    def getKeyVar(self, actor, keyName):
        """Return a keyVar by this name and actor.
        
        If there are multiple matching keyVars returns the first registered;
        to get a different keyVar use getKeyVarList.
        
        Raise LookupError if no such keyword found.
        """
        keyVarList = self.getKeyVarList(actor, keyName)
        if not keyVarList:
            raise LookupError("Could not find a keyVar with actor=%s, keyName=%s" % (actor, keyName))
        return keyVarList[0]
                
    def removeKeyVar(self, keyVar):
        """Remove the specified keyword variable, returning the KeyVar if removed, else None

        See also "addKeyVar".

        Inputs:
        - keyVar: the keyword variable to remove

        Returns:
        - the removed keyVar, if present, None otherwise.
        """
        dictKey = self._makeDictKey(keyVar.actor, keyVar.name)
        keyVarList = self.keyVarListDict.get(dictKey, [])
        if keyVar in keyVarList:
            keyVarList.remove(keyVar)
            return keyVar
        else:
            return None

    @staticmethod
    def _makeDictKey(actor, keyName):
        """Make a keyVarListDict key out of an actor and keyword name
        """
        return (actor.lower(), keyName.lower())

if __name__ == "__main__":
    print "\nDemonstrating KeyVarDispatcher\n"
    import time
    import opscore.protocols.types as protoTypes
    import opscore.protocols.keys as protoKeys
    import twisted.internet.reactor
    
    kvd = KeyVarDispatcher()

    def showVal(keyVar):
        print "keyVar %s.%s = %r, isCurrent = %s" % (keyVar.actor, keyVar.name, keyVar.valueList, keyVar.isCurrent)

    # scalars
    keyList = (
        protoKeys.Key("StringKey", protoTypes.String()),
        protoKeys.Key("IntKey", protoTypes.Int()),
        protoKeys.Key("FloatKey", protoTypes.Float()),
        protoKeys.Key("BooleanKey", protoTypes.Bool("F", "T")),
        protoKeys.Key("KeyList", protoTypes.String(), protoTypes.Int()),
    )
    keyVarList = [keyvar.KeyVar("test", key) for key in keyList]
    for keyVar in keyVarList:
        keyVar.addCallback(showVal)
        kvd.addKeyVar(keyVar)
    
    dataList = [
        "StringKey=hello",
        "IntKey=1",
        "FloatKey=1.23456789",
        "BooleanKey=T",
        "KeyList=three, 3",
        "Coord2Key=45.0, 0.1, 32.1, -0.1, %s" % (time.time(),),
    ]
    dataStr = "; ".join(dataList)

    replyStr = "myprog.me 11 test : " + dataStr
    print "\nDispatching message correctly; CmdVar done so only KeyVar callbacks should be called:"
    kvd.dispatchReplyStr(replyStr)
    
    twisted.internet.reactor.run()
