"""
Performs runtime configration based on command-line options and INI files
"""

# Created 8-Apr-2009 by David Kirkby (dkirkby@uci.edu)

import sys
import os,os.path
import optparse
import ConfigParser

class ConfigError(Exception):
    pass

class ConfigOptionParser(optparse.OptionParser):
    """
    A command-line options parser that takes defaults from INI files
    """
    def __init__(self,*args,**kwargs):
        # look for an optional 'configfile' keyword arg that specifies the
        # INI file containing our configuration defaults
        configFileName = kwargs.get('config_file','config.ini')
        self.configSectionName = kwargs.get('config_section','DEFAULT')
        # strip out our special options
        if 'config_file' in kwargs:
            del kwargs['config_file']
        if 'config_section' in kwargs:
            del kwargs['config_section']
        # build a search path for the configfile
        configFiles = [
            os.path.join(sys.path[0],configFileName), # current program's path
            os.path.join(os.getcwd(),configFileName)  # current working dir
        ]
        # read all available INI config options
        self.configParser = ConfigParser.SafeConfigParser()
        self.configParser.read(configFiles)
        # initialize our list of secret option names
        self.secretOptions = [ ]
        # initialize our base class
        optparse.OptionParser.__init__(self,*args,**kwargs)

    def add_option(self,*args,**kwargs):
        """
        Adds an option for command line processing
        
        Obtains a default value from an INI file if one is available. Implements
        a special 'secret' type for handling encrypted strings.
        """
        # what type of value does this option expect
        getter = self.configParser.get
        if 'action' in kwargs and kwargs['action'] in ('store_true','store_false'):
            getter = self.configParser.getboolean
        # loop over option aliases
        for alias in args:
            if alias[:2] != '--':
                continue
            # lookup each long-form option name in turn, until we get a match
            optionName = alias[2:]
            try:
                default = getter(self.configSectionName,optionName)
                kwargs['default'] = default
                break
            except (ConfigParser.NoOptionError,ConfigParser.NoSectionError):
                pass
        # is this a secret option?
        optionType = kwargs.get('type','string')
        if optionType == 'secret':
            if not 'dest' in kwargs:
                raise optparse.OptionValueError('A secret option must specify a destination')
            self.secretOptions.append(kwargs['dest'])
            kwargs['type'] = 'string'
            pass
        # do the normal option processing
        return optparse.OptionParser.add_option(self,*args,**kwargs)
        
    def parse_args(self,args=None,values=None,
        passphrase=None,prompt='Enter the pass phrase: '):
        """
        Parses command line arguments
        """
        # do the normal parsing
        (options,args) = optparse.OptionParser.parse_args(self,args,values)
        # is there is any secret data that needs decrypting?
        if self.secretOptions:
            import getpass
            # only introduce these external dependencies if necessary
            from Crypto.Hash import MD5 as hasher
            from Crypto.Cipher import AES as cipher
            if not passphrase:
                passphrase = getpass.getpass(prompt)
            key = hasher.new(passphrase).digest()
            assert(len(key) in [16,24,32])
            engine = cipher.new(key,cipher.MODE_ECB)
            for secret in self.secretOptions:
                data = engine.decrypt(ConfigOptionParser.hex2bin(getattr(options,secret)))
                npad = ord(data[-1])
                if npad <= 0 or npad > cipher.block_size or data[-npad:-1] != '\x00'*(npad-1):
                    raise optparse.OptionValueError('badly formed value for %s' % secret)
                setattr(options,secret,data[0:-npad])
        # return the parse results
        return (options,args)
    
    @staticmethod    
    def bin2hex(data):
        """
        Returns an ASCII hexadecimal representation of binary data
        """
        bytes = ['%02x' % ord(c) for c in data]
        return ''.join(bytes)

    @staticmethod
    def hex2bin(data):
        """
        Returns the inverse of bin2hex()
        """
        if not len(data) % 2 == 0:
            raise ConfigError('hex digest must have even length')
        try:
            bytes = [ ]
            for index in xrange(len(data)/2):
                bytes.append(chr(int(data[2*index:2*(index+1)],16)))
            return ''.join(bytes)
        except ValueError:
            raise ConfigError('badly formmated hex digest')