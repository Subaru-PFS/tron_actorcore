#!/usr/bin/env python
"""Message code severity
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
        elif code in ReplyHeader.WarningCodes:
            retDict[code] = RO.Constants.sevWarning
        else:
            retDict[code] = RO.Constants.sevNormal
        return retDict

# Message code severity dictionary
# key: message code (an opscore.protocols.messages.ReplyHeader.MsgCode)
# value: severity (an RO.Constants.sevX constant)
MsgCodeSeverityDict = _makeSeverityDict()
