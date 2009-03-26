#!/usr/bin/env python
"""Define valid hub message types, groupings of message types, and a mapping to severity.
"""
import RO.Constants

# TypeDict translates message type characters to message categories
# entries are: (meaning, category), where:
# meaning is used for messages displaying what's going on
# category is coarser and is used for filtering by category
TypeDict = {
    "!":("fatal error", RO.Constants.sevError), # a process dies
    "f":("failed", RO.Constants.sevError), # command failed
    "w":("warning", RO.Constants.sevWarning),
    "i":("information", RO.Constants.sevNormal), # the initial state
    "s":("status", RO.Constants.sevNormal), # deprecated
    ">":("queued", RO.Constants.sevNormal),
    ":":("finished", RO.Constants.sevNormal),
}
# all message types
AllTypes = "".join(TypeDict.keys())
# useful other message types
DoneTypes = ":f!"
FailTypes = "f!"
