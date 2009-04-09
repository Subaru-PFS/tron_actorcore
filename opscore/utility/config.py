"""
Performs runtime configration based on command-line options and INI files
"""

# Created 8-Apr-2009 by David Kirkby (dkirkby@uci.edu)

import sys
import os,os.path
import optparse
import ConfigParser

class ConfigOptionParser(optparse.OptionParser):
    """
    A command-line options parser that takes defaults from INI files
    """
    def __init__(self,*args,**kwargs):
        # look for an optional 'configfile' keyword arg that specifies the
        # INI file containing our configuration defaults
        configFileName = kwargs.get('configFile','config.ini')
        self.configSectionName = kwargs.get('configSection','DEFAULT')
        # strip out our special options
        if 'configFile' in kwargs:
            del kwargs['configFile']
        if 'configSection' in kwargs:
            del kwargs['configSection']
        # build a search path for the configfile
        configFiles = [
            os.path.join(sys.path[0],configFileName), # current program's path
            os.path.join(os.getcwd(),configFileName)  # current working dir
        ]
        # read all available INI config options
        self.configParser = ConfigParser.SafeConfigParser()
        self.configParser.read(configFiles)
        # initialize our base class
        optparse.OptionParser.__init__(self,*args,**kwargs)

    def add_option(self,*args,**kwargs):
        # loop over option aliases
        for alias in args:
            if alias[:2] != '--':
                continue
            optionName = alias[2:]
            # look for a config file default for this option
            try:
                default = self.configParser.get(self.configSectionName,optionName)
                kwargs['default'] = default
            except ConfigParser.NoOptionError:
                pass
        return optparse.OptionParser.add_option(self,*args,**kwargs)
