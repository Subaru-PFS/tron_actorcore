#!/usr/bin/env python
"""Define valid hub message types, groupings of message types, and a mapping to severity.
"""
import opscore.protocols.messages
import RO.Constants

def _makeSeverityDict():
    ReplyHeader = opscore.protocols.messages.ReplyHeader
    
    retDict = dict()
    codeDict = ReplyHeader.MsgCode.enumValues
    for code in codeDict:
        if code in ReplyHeader.FailedCodes:
            retDict[code] = RO.Constants.sevError
        elif code == "WARNING":
            retDict[code] = RO.Constants.sevWarning
        else:
            retDict[code] = RO.Constants.sevNormal
        return retDict

# SeverityDict translates message type codes to message categories
MsgCodeSeverityDict = _makeSeverityDict()
