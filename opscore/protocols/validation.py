"""
Validation of SDSS-3 command and reply protocols

Refer to https://trac.sdss3.org/wiki/Ops/Validation for details.
"""

# Created 7-Nov-2008 by David Kirkby (dkirkby@uci.edu)

import sys
import textwrap

import opscore.protocols.messages as protoMess
import opscore.protocols.keys as protoKeys

from opscore.protocols.keys import Consumer,TypedValues,Key,CmdKey,KeysManager
from opscore.protocols.keysformat import KeysFormatParser

class ValidationError(Exception):
    pass
    
class KeywordsIterator(object):
    """
    Iterates through a Keywords instance
    """
    def __init__(self,keywords,begin=0):
        self.keys = keywords
        self.index = begin
    
    def keyword(self):
        try:
            return self.keys[self.index]
        except IndexError:
            return None

    def advance(self,amount=1):
            self.index += amount
            
    def __repr__(self):
        return '[%s]' % ','.join([repr(key) for key in self.keys[self.index:]])
        
    def clone(self):
        # this could be optimized to only clone keys[index:]
        return KeywordsIterator(self.keys.clone(),self.index)
    
    def copy(self,other):
        self.keys.copy(other.keys)
        self.index = other.index

class DispatchMixin(object):

    def __rshift__(self,callback):
        if not callable(callback):
            raise ValidationError('invalid callback %r' % callback)
        if not hasattr(self,'callbacks'):
            self.callbacks = [ ]
        self.callbacks.append(callback)
        return self
    
    def invokeCallbacks(self,message):
        if hasattr(self,'callbacks'):
            for callback in self.callbacks:
                callback(message)

class Cmd(Consumer,DispatchMixin):
    
    parser = None

    def __init__(self,verb,*args,**metadata):
        # Initialize a shared format parser. This must be done within our ctor
        # rather than at module load time so we can capture the caller's context.
        if not Cmd.parser:
            Cmd.parser = KeysFormatParser()
        self.verb = verb
        self.typedValues = TypedValues(args[:-1])
        self.consumer = None
        if args:
            self.format = args[-1]
        else:
            self.format = ''
        if self.format:
            self.consumer = self.parser.parse(self.format)
        self.help = metadata.get('help',None)
        
    def __repr__(self):
        return 'Cmd(%r,%r,%r)' % (self.verb,self.typedValues,self.format)
        
    def consume(self,message):
        self.trace(message)
        if not isinstance(message,protoMess.Command):
            return self.failed('message is not a command')
        if not message.name == self.verb:
            return self.failed('command has wrong verb')
        # match any command values
        if not self.typedValues.consume(message.values):
            return self.failed('no match for command values')
        # remember the current state of our parsed command in case we need to
        # restore it after an incomplete keywords validation
        self.checkpoint = message.clone()
        # try to match all of this command's keywords against our format string
        iterator = KeywordsIterator(message.keywords)
        if self.consumer and not self.consumer.consume(iterator):
            # restore the original message
            message.copy(self.checkpoint)
            return self.failed('keywords do not match format string')
        if iterator.keyword():
            # restore the original message
            message.copy(self.checkpoint)
            return self.failed('command has unmatched keywords: %r' % iterator)
        # if we get here, the message has been fully validated
        self.invokeCallbacks(message)
        return self.passed(message)
        
    def match(self,message):
        self.trace(message)
        if not isinstance(message,protoMess.Command):
            return self.failed('message is not a command')
        if not message.name == self.verb:
            return self.failed('command has wrong verb')
        # match any command values
        if not self.typedValues.consume(message.values):
            return self.failed('no match for command values')
        # remember the current state of our parsed command in case we need to
        # restore it after an incomplete keywords validation
        self.checkpoint = message.clone()
        # try to match all of this command's keywords against our format string
        iterator = KeywordsIterator(message.keywords)
        if self.consumer and not self.consumer.consume(iterator):
            # restore the original message
            message.copy(self.checkpoint)
            return self.failed('keywords do not match format string')
        if iterator.keyword():
            # restore the original message
            message.copy(self.checkpoint)
            return self.failed('command has unmatched keywords: %r' % iterator)
        # if we get here, the message has been fully validated

        return message, self.callbacks
        
    def create(self,*kspecs,**kwargs):
        values = kwargs.get('values',None)
        if len(kspecs) == 1 and isinstance(kspecs[0],list):
            kspecs = kspecs[0]
        keywords = [ ]
        for kspec in kspecs:
            if isinstance(kspec,str):
                keywords.append(protoMess.Keyword(kspec))
            elif isinstance(kspec,tuple):
                key = CmdKey.getKey(kspec[0])
                vlist = list(kspec[1:])
                if len(vlist) == 1 and isinstance(vlist[0],list):
                    vlist = vlist[0]
                keywords.append(key.create(vlist))
            else:
                raise ValidationError('Invalid keyword specification: %r' % kspec)
        command = protoMess.Command(self.verb,values,keywords)
        if not self.consume(command):
            raise ValidationError('Keywords or values do match Cmd template')
        return command
    
    def describe(self):
        text = '%12s: %s\n' % ('Command',self.verb)
        if self.help:
            pad = '\n' + ' '*14
            formatted = textwrap.fill(textwrap.dedent(self.help),width=66).replace('\n',pad)
            text += '%12s: %s\n' % ('Description',formatted)
        text += self.typedValues.describe()[:-1] + '\n'
        text += '%12s: %s\n' % ('Keywords',self.format)
        return text
    
    def describeAsHTML(self):
        pass

import opscore.protocols.parser as parser

class HandlerBase(object):
    """
    Base class for Command and Reply handlers
    
    Subclass must define parser attribute and consumeLine method
    """
    def consume(self,string):
        for line in string.split('\n'):
            # ignore empty lines
            if not line.strip():
                continue
            # try to parse this line
            try:
                parsed = self.parser.parse(line)
                self.consumeLine(parsed)
            except parser.ParseError:
                pass

    def match(self,line):
        # try to parse this line
        try:
            parsed = self.parser.parse(line)
            return self.matchLine(parsed)
        except parser.ParseError:
            return None, []

class CommandHandler(HandlerBase):
    """
    Validates command strings and invokes any registered callbacks
    """
    def __init__(self,*consumers):
        self.consumers = { }
        for consumer in consumers:
            if not isinstance(consumer,Cmd):
                raise ValidationError('CommandHandler only accepts Cmds')
            name = consumer.verb
            if name not in self.consumers:
                self.consumers[name] = [ ]
            self.consumers[name].append(consumer)
        self.parser = parser.CommandParser()
    
    def consumeLine(self,parsed):
        if not parsed.name in self.consumers:
            raise ValidationError("No handler for cmd: %s" % parsed.name)
        for consumer in self.consumers[parsed.name]:
            if consumer.consume(parsed):
                return
        raise ValidationError("Invalid cmd: %s" % parsed.canonical())

    def matchLine(self,parsed):
        if not parsed.name in self.consumers:
            raise ValidationError("No handler for cmd: %s" % parsed.name)
        for consumer in self.consumers[parsed.name]:
            retval = consumer.match(parsed)
            if retval:
                return retval
        raise ValidationError("Invalid cmd: %s" % parsed.canonical())

class ReplyKey(Consumer,DispatchMixin,KeysManager):
    """
    Consumes a reply keyword
    """
    def __init__(self,name):
        self.key = ReplyKey.getKey(name)

    def __repr__(self):
        return 'ReplyKey(%s)' % self.key.name

    def consume(self,keyword):
        self.trace(keyword)
        if not self.key.consume(keyword):
            return self.failed('no match for reply keyword')
        self.invokeCallbacks(keyword)
        return self.passed(keyword)

class ReplyHandler(HandlerBase):
    """
    Validates reply strings and invokes any registered callbacks
    """
    def __init__(self,*consumers):
        self.consumers = { }
        for consumer in consumers:
            if not isinstance(consumer,ReplyKey):
                raise ValidationError('ReplyHandler only accepts ReplyKeys')
            name = consumer.key.name
            if name in self.consumers:
                raise ValidationError("Duplicate consumer for reply keyword '%s'" % name)
            self.consumers[name] = consumer
        self.parser = parser.ReplyParser()

    def consumeLine(self,parsed):
        iterator = KeywordsIterator(parsed.keywords)
        next = iterator.keyword()
        while next:
            if next.name in self.consumers:
                if not self.consumers[next.name].consume(next):
                    raise ValidationError("Handler failed for keyword %r" % next)
            else:
                # silently move on if we don't have a handler for this keyword
                pass
            iterator.advance()
            next = iterator.keyword()
    
class Trace(object):
    """
    A callable object for tracing the execution of a message handler
    """
    def __init__(self,label=None):
        self.label = label
    
    def __call__(self,message):
        if self.label:
            print '%s: %s' % (self.label,message.canonical())
        else:
            print message.canonical()
