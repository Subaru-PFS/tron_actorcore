"""
Keys format string parser for SDSS-3 command validation

Refer to https://trac.sdss3.org/wiki/Ops/Validation for details.
"""

# Created 18-Nov-2008 by David Kirkby (dkirkby@uci.edu)

from builtins import object
from opscore.protocols.keys import Consumer,Key,CmdKey,RawKey

import ply.lex as lex
import ply.yacc as yacc

class KeysFormatParseError(Exception):
    pass

class Group(Consumer):
    """
    Consumes a group of command keywords
    """
    def __init__(self,positioned,floating):
        self.positioned = positioned
        self.floating = floating

    def __repr__(self):
        return 'Group{%s;%s}' % (
            ','.join([repr(x) for x in self.positioned]),
            ','.join([repr(x) for x in self.floating])
        )

    def consume(self,where):
        self.trace(where)
        # loop over positioned initial keys
        for key in self.positioned:
            if not key.consume(where):
                return self.failed('missing positioned %s' % key)
        # try to match floating keys
        for key in self.floating:
            key.used = False
        next = where.keyword()
        while next:
            # loop over our floating keywords
            for key in self.floating:
                # a key can only be used once
                if key.used:
                    continue
                # does this key consume anything?
                if key.consume(where) and next != where.keyword():
                    key.used = True
                    break
            # did we consume any keywords during the last pass through our floating keys?
            if next == where.keyword():
                break
            next = where.keyword()
        # check that all required floating keys matched
        for key in self.floating:
            if not isinstance(key,Optional) and not key.used:
                if next:
                    return self.failed('missing floating %s before %s' % (key,next))
                else:
                    return self.failed('missing floating %s' % key)
        return self.passed(where)

class OneOf(Consumer):
    """
    Tries two or more alternatives to consume a command keyword
    """
    def __init__(self,*keys):
        self.keys = list(keys)

    def __repr__(self):
        return '(%s)' % '|'.join([repr(key) for key in self.keys])

    def consume(self,where):
        self.trace(where)
        # remember the current state of the next keyword in case we have to
        # restore it after an incomplete keys validation
        keyword = where.keyword()
        self.checkpoint = keyword.clone()
        for key in self.keys:
            if key.consume(where):
                return self.passed(where)
        # restore the keyword to its original state
        keyword.copy(self.checkpoint)
        return self.failed('no keys match %s' % keyword)

class Optional(Consumer):
    """
    Declares that a consumer is optional
    """
    def __init__(self,group):
        self.group = group

    def __repr__(self):
        return '?%r' % self.group

    def consume(self,where):
        self.trace(where)
        # remember the current state of the remaining keywords in case we have to
        # restore them after a failed validation
        self.checkpoint = where.clone()
        if not self.group.consume(where):
            # restore the remaining keywords to their original state
            where.copy(self.checkpoint)
        return self.passed(where)

class KeysFormatParser(object):
    """
    Parses a command keyword format string
    """
    debug = False

    # single-character literals
    literals = "()[]<>@|"

    # inline whitespace
    t_WS = r'[ \t]+'

    # a keyword name
    t_NAME = r'[A-Za-z][A-Za-z0-9._]*'

    # lexical tokens
    tokens = ('WS','NAME')

    def p_KeysGroup(self,t):
        "Group : Positioned WS Floating"
        t[0] = Group(t[1],t[3])

    def p_KeysGroupNoPositioned(self,t):
        "Group : Floating"
        t[0] = Group([],t[1])

    def p_KeysGroupNoFloating(self,t):
        "Group : Positioned"
        t[0] = Group(t[1],[])

    def p_PositionedKey(self,t):
        "Positioned : '@' Keyword"
        t[0] = [t[2]]

    def p_PositionedKeys(self,t):
        "Positioned : Positioned WS '@' Keyword"
        t[0] = t[1]
        t[0].append(t[4])

    def p_FloatingKey(self,t):
        "Floating : Keyword"
        t[0] = [t[1]]

    def p_FloatingKeys(self,t):
        "Floating : Floating WS Keyword"
        t[0] = t[1]
        t[0].append(t[3])

    def p_KeywordOptionalGroup(self,t):
        "Keyword : '[' Group ']'"
        t[0] = Optional(t[2])

    def p_KeywordRequiredGroup(self,t):
        "Keyword : '(' Group ')'"
        t[0] = t[2]

    def p_NonGroupedKeyword(self,t):
        """Keyword : KeywordOr
                   | OneKeyword"""
        t[0] = t[1]

    def p_KeywordOrAppend(self,t):
        "KeywordOr : KeywordOr '|' OneKeyword"
        t[0] = t[1]
        t[0].keys.append(t[3])

    def p_KeywordOr(self,t):
        "KeywordOr : OneKeyword '|' OneKeyword"
        t[0] = OneOf(t[1],t[3])

    def p_KeywordRef(self,t):
        "OneKeyword : '<' NAME '>'"
        t[0] = CmdKey(CmdKey.getKey(t[2]))

    def p_KeywordByName(self,t):
        "OneKeyword : NAME"
        if t[1].lower() == 'raw':
            t[0] = RawKey()
        else:
            t[0] = CmdKey(Key(t[1]))

    def p_error(self,tok):
        """
        Handles parse errors
        """
        if not tok:
            raise KeysFormatParseError("Unable to parse format string")
        raise KeysFormatParseError("Unexpected %s token in format string" % tok.type)

    def t_error(self,tok):
        """
        Handles lexical analysis errors
        """
        raise KeysFormatParseError("Unable to split format string into tokens")

    def tokenize(self,string):
        """
        Generates the lexical tokens found in a format string
        """
        string = string.rstrip('\n')
        self.lexer.input(string)
        tok = self.lexer.token()
        while tok:
            yield tok
            tok = self.lexer.token()

    def parse(self,string):
        """
        Returns the parsed representation of a format string
        """
        self.string = string.rstrip('\n')
        return self.engine.parse(self.string,lexer=self.lexer,debug=self.debug)

    def __init__(self):
        """
        Creates a new keywords format string parser
        """
        self.lexer = lex.lex(object=self,debug=self.debug)
        self.engine = yacc.yacc(module=self,debug=self.debug,write_tables=0)
