#!/usr/bin/env python

""" Wrap top-level ACTOR functions. """

import pdb
import logging
import pprint
import sys
import ConfigParser

import opscore.protocols.validation as validation
import opscore.protocols.keys as keys
import opscore.protocols.types as types

from opscore.utility.qstr import qstr
from opscore.utility.tback import tback

class CoreCmd(object):
    """ Wrap common Actor commands """
    
    def __init__(self, actor):
        self.actor = actor

        #
        # Set the keyword dictionary
        #
        self.keys = keys.KeysDictionary("actorcore_core", (1, 1),
                                        keys.Key("cmds", types.String()*(1,None),
                                                 help="A list of command modules."),
                                        )

        self.vocab = (
            ('help', '[<cmds>]', self.cmdHelp),
            ('reload', '[<cmds>]', self.reloadCommands),
            ('reloadConfiguration', '', self.reloadConfiguration),
            ('exitexit', '', self.exitCmd),
        )

    def cmdHelp(self, cmd):
        """ List whatever command key info we can extract.. """

        cmdKeys = cmd.cmd.keywords
        
        handler = self.actor.handler
        helpList = []
        for verb in handler.consumers.keys():
            clist = handler.consumers[verb]
            for c in clist:
                callbacks = c.callbacks
                callback = callbacks[0]
                if len(callbacks) > 1:
                    cmd.warn('text="skipping help for some callbacks for %s %s"' % (verb, c.format))

                # Root around for help info.
                callbackDoc = callback.__doc__
                if not callbackDoc:
                    callbackDoc = "no help"
                oneLiner = callbackDoc.split('\n',1)[0].strip()
                helpList.append("%s %s -- %s" %
                                (verb, c.format, oneLiner))
                
                # Sometimes add the rest of the doc string....

        helpList.sort()
        for h in helpList:
            cmd.inform('text=%s' % (qstr(h)))
        cmd.finish()
                                                                    
    def reloadCommands(self,cmd):
        """ If cmds defined, define the listed commands, otherwise reload all command sets. """

        if 'cmds' in cmd.cmd.keywords:
            # Load the specified module
            commands = cmd.cmd.keywords['cmds'].values
            for command in commands:
                cmd.respond('text="Attaching %s."' % (command))
                self.actor.attachCmdSet(command)
        else:
            # Load all modules
            cmd.respond('text="Attaching all command sets."')
            self.actor.attachAllCmdSets()

        cmd.finish('')
    
    def reloadConfiguration(self,cmd):
        """ Reload the configuration. """
        cmd.respond('text="Reparsing the configuration file: %s."' % (self.actor.configFile))
        logging.warn("reading config file %s", self.actor.configFile)

        try:
            newConfig = ConfigParser.ConfigParser()
            newConfig.read(self.actor.configFile)
        except Exception, e:
            cmd.fail('text=%s' % (qstr("failed to read the configuration file, old config untouched: %s" % (e))))
            return
        
        self.actor.config = newConfig
        cmd.finish('text="reloaded configuration file')
    
    def exitCmd(self, cmd):
        """ Brutal exit when all else has failed. """
        from twisted.internet import reactor

        reactor.stop()
        sys.exit(0)
