#!/usr/bin/env python

""" Wrap top-level ACTOR functions. """

import pdb
import logging
import pprint
import re
import sys
import ConfigParser

import opscore.protocols.validation as validation
import opscore.protocols.keys as keys
import opscore.protocols.types as types
import actorcore.help as help
reload(help)

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
                                        keys.Key("cmd", types.String(), help="A command name"),
                                        keys.Key("cmds", types.String()*(1,None),
                                                 help="A list of command modules."),
                                        keys.Key("html", help="Generate HTML"),
                                        keys.Key("full", help="Generta full help for all commands"),
                                        keys.Key("pageWidth", types.Int(),
                                                 help="Number of characters per line"),
                                        )

        self.vocab = (
            ('help', '[(full)] [<cmd>] [<pageWidth>] [(html)]', self.cmdHelp),
            ('reload', '[<cmds>]', self.reloadCommands),
            ('reloadConfiguration', '', self.reloadConfiguration),
            ('version', '', self.version),
            ('exitexit', '', self.exitCmd),
            ('ipdb', '', self.ipdbCmd),
            ('ipython', '', self.ipythonCmd),
        )

    def cmdHelp(self, cmd):
        """Return a help string for command name, formatted for a line length of pageWidth"""

        if "cmd" in cmd.cmd.keywords:
            cmds = [cmd.cmd.keywords['cmd'].values[0]]
            fullHelp = True
        else:
            cmds = []
            for a, cSet in self.actor.commandSets.items():
                if a != "CoreCmd":
                    cmds += [c[0] for c in cSet.vocab]
            fullHelp = False
            
        if "full" in cmd.cmd.keywords:
            fullHelp = True

        pageWidth = int(cmd.cmd.keywords['pageWidth'].values[0]) if "pageWidth" in cmd.cmd.keywords else 80
        html = "html" in cmd.cmd.keywords

        first = True
        for cmdName in cmds:
            helpStr = ""
            for csetName, cSet in self.actor.commandSets.items():
                if cmdName in [c[0] for c in cSet.vocab]:
                    helpStr = help.help(self.actor.name, cmdName, cSet.vocab, cSet.keys, pageWidth, html,
                                        fullHelp=fullHelp)
                    break

            if not helpStr:
                cmd.warn('text="I\'m afraid that I can\'t help you with %s"' % cmdName)
                continue

            if first:
                first = False
            elif fullHelp:
                cmd.inform('help=%s' % qstr("--------------------------------------------------"))

            for line in helpStr.split('\n'):
                cmd.inform('help=%s' % qstr(line))

        cmd.finish("")
                                          
    def version(self, cmd, doFinish=True):
        """ Return a version keyword. """

        versionString = self.actor.versionString(cmd)
        if doFinish:
            cmd.finish('version=%s' % (qstr(versionString)))
        else:
            cmd.respond('version=%s' % (qstr(versionString)))

    def reloadCommands(self, cmd):
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

        # Finish by redeclaring the version, since we are probably a live version.
        self.version(cmd)
    
    def reloadConfiguration(self, cmd):
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

    def ipdbCmd(self, cmd):
        """ Try to start some debugger. """

        try:
            import IPython.core.debugger as iPdb
            debug_here = iPdb.Tracer()
            cmd.warn('text="starting ipdb on console..."')
            debugFunction = debug_here
        except Exception, e:
            import pdb
            cmd.warn('text="starting pdb on console..."')
            debugFunction = pdb.set_trace
            
        try:
            debugFunction()
        except Exception, e:
            cmd.fail('text="debugger blammo: %s"' % (e))
            return
        
        cmd.warn('text="back to regular operations"')
        cmd.finish()

    def ipythonCmd(self, cmd):
        """ Try to start a subshell. """
        
        try:
            from IPython import embed
        except Exception, e:
            cmd.fail('text="failed to start ipython: %s"' % (e))
            return

        cmd.warn('text="starting ipython on console..."')
        try:
            embed() # this call anywhere in your program will start IPython
        except Exception, e:
            cmd.fail('text="ipython blammo: %s"' % (e))
            return
        
        cmd.warn('text="back to normal interpreter"')
        cmd.finish()
