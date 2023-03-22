#!/usr/bin/env python

""" Wrap top-level ACTOR functions. """

import re
import sys

import actorcore.help as help
import opscore.protocols.keys as keys
import opscore.protocols.types as types
from opscore.utility.qstr import qstr

try:
    from importlib import reload
except:
    pass

reload(help)


class CoreCmd(object):
    """ Wrap common Actor commands """

    def __init__(self, actor):
        self.actor = actor

        #
        # Set the keyword dictionary
        #
        self.keys = keys.KeysDictionary("actorcore_core", (1, 2),
                                        keys.Key("cmd", types.String(), help="A command name"),
                                        keys.Key("controller", types.String(),
                                                 help='the names a controller.'),
                                        keys.Key("name", types.String(),
                                                 help='an optional name to assign to a controller instance'),
                                        keys.Key("filename", types.String(),
                                                 help='file for output'),
                                        keys.Key("cmds", types.String(),
                                                 help="A regexp matching commands"),
                                        keys.Key("html", help="Generate HTML"),
                                        keys.Key("full", help="Generate full help for all commands"),
                                        keys.Key("pageWidth", types.Int(),
                                                 help="Number of characters per line"),
                                        )

        self.vocab = (
            ('help', '[(full)] [<cmds>] [<pageWidth>] [(html)]', self.cmdHelp),
            ('cardFormats', '<filename>', self.cardFormats),
            ('reload', '[<cmds>]', self.reloadCommands),
            ('reloadConfiguration', '', self.reloadConfiguration),
            ('connect', '<controller> [<name>]', self.connect),
            ('disconnect', '<controller>', self.disconnect),
            ('version', '', self.version),
            ('exitexit', '', self.exitCmd),
            ('ipdb', '', self.ipdbCmd),
            ('ipython', '', self.ipythonCmd),
        )

    def controllerKey(self):
        """ Return formatted keyword listing all loaded controllers. """

        controllerNames = list(self.actor.controllers.keys())
        key = 'controllers=%s' % (','.join([c for c in controllerNames]) if controllerNames else None)

        return key

    def connect(self, cmd):
        """ Reload controller objects. 

        If the controllers argument is not passed in, all controllers are reloaded.
        """

        controller = cmd.cmd.keywords['controller'].values[0]
        try:
            instanceName = cmd.cmd.keywords['name'].values[0]
        except:
            instanceName = controller

        try:
            self.actor.attachController(controller,
                                        instanceName=instanceName,
                                        cmd=cmd)
        except:
            cmd.warn(self.controllerKey())
            cmd.warn('text="failed to connect controller %s' % instanceName)
            raise

        cmd.finish(self.controllerKey())

    def disconnect(self, cmd):
        """ Disconnect the given, or all, controller objects. """

        controller = cmd.cmd.keywords['controller'].values[0]

        try:
            self.actor.detachController(controller,
                                        cmd=cmd)
        except:
            cmd.warn(self.controllerKey())
            cmd.warn('text="failed to disconnect controller %s"')
            raise

        cmd.finish(self.controllerKey())

    def cardFormats(self, cmd):
        """ Generate the FITS format file for all connected models. """

        try:
            from ics.utils.fits import mhs as fitsUtils
        except ImportError:
            cmd.fail('text="failed to import ics_utils FITS routines"')
            return

        filename = cmd.cmd.keywords['filename'].values[0]
        fitsUtils.printHeaderFormats(cmd, self.actor, filename, longNames=True)
        cmd.finish()

    def _vocabCmds(self, vocab):
        return [' '.join((c[0], c[1])).strip() for c in vocab]

    def cmdHelp(self, cmd):
        """ Return a summary of all commands, or the complete help string for the specified commands.

        Also allows generating an html file.
        """

        cmds = []
        for a, cSet in list(self.actor.commandSets.items()):
            cmds.extend(self._vocabCmds(cSet.vocab))
        fullHelp = False
        cmds.sort()

        if "cmds" in cmd.cmd.keywords:
            cmdRe = re.compile(cmd.cmd.keywords['cmds'].values[0])
            cmds = filter(cmdRe.search, cmds)
            fullHelp = True

        if "full" in cmd.cmd.keywords:
            fullHelp = True

        pageWidth = int(cmd.cmd.keywords['pageWidth'].values[0]) if "pageWidth" in cmd.cmd.keywords else 80
        html = "html" in cmd.cmd.keywords

        first = True
        for cmdName in cmds:
            helpList = []
            for csetName, cSet in list(self.actor.commandSets.items()):
                if cmdName in self._vocabCmds(cSet.vocab):
                    try:
                        helpStr = help.help(self.actor.name, cmdName, cSet.vocab, cSet.keys, pageWidth, html,
                                            fullHelp=fullHelp)
                    except Exception as e:
                        helpStr = "something went wrong when building help for %s: %s" % (cmdName, e)
                        cmd.warn('text=%s' % (qstr(helpStr)))
                    helpList.append(helpStr)

            if not helpList:
                cmd.warn('text="I\'m afraid that I can\'t help you with %s"' % cmdName)
                continue

            if first:
                first = False
            elif fullHelp:
                cmd.inform('help=%s' % qstr("--------------------------------------------------"))

            for helpChunk in helpList:
                for line in helpChunk.split('\n'):
                    cmd.inform('help=%s' % qstr(line))

        cmd.finish("")

    def version(self, cmd, doFinish=True):
        """ Return a version keyword. """

        self.actor.sendVersionKey(cmd)
        if doFinish:
            cmd.finish()

    def reloadCommands(self, cmd):
        """ If cmds argument is defined, reload the listed command sets, otherwise reload all command sets. """

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
        """ Reload the configuration.

        Note that only some configuration variables will take effect, depending on how
        they are used. Read the Source, etc.
        """
        # still referring to old config for now if it exists.
        if self.actor.etcConfigExists:
            cmd.respond('text="Reparsing etc config file: %s."' % self.actor.configFile)

        cmd.respond('text="Reparsing the configuration file: %s."' % self.actor.actorConfig.filepath)

        try:
            self.actor._reloadConfiguration(cmd)
        except Exception as e:
            cmd.fail('text="failed to reload configuration file: %s', e)
            return

        cmd.finish('text="configuration file reloaded"')

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
        except Exception as e:
            import pdb
            cmd.warn('text="starting pdb on console..."')
            debugFunction = pdb.set_trace

        try:
            debugFunction()
        except Exception as e:
            cmd.fail('text="debugger blammo: %s"' % (e))
            return

        cmd.warn('text="back to regular operations"')
        cmd.finish()

    def ipythonCmd(self, cmd):
        """ Try to start a subshell. """

        try:
            from IPython import embed
        except Exception as e:
            cmd.fail('text="failed to start ipython: %s"' % (e))
            return

        cmd.warn('text="starting ipython on console..."')
        try:
            embed()  # this call anywhere in your program will start IPython
        except Exception as e:
            cmd.fail('text="ipython blammo: %s"' % (e))
            return

        cmd.warn('text="back to normal interpreter"')
        cmd.finish()

    def iKernelCmd(self, cmd):
        """ Try to start a subshell. """

        try:
            import twistedloop
        except Exception as e:
            cmd.fail('text="failed to import twistedloop: %s"' % (e))
            return

        cmd.warn('text="starting ipython kernel ..."')
        try:
            twistedloop.startloop()
        except Exception as e:
            cmd.fail('text="ipython blammo: %s"' % (e))
            return

        cmd.finish('text="back to normal interpreter"')
        cmd.finish()
