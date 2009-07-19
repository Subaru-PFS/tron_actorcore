"""
Performs runtime configration based on command-line options and INI files

Refer to https://trac.sdss3.org/wiki/Ops/Config for details.
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
        productName = kwargs.get('product_name',None)
        configFileName = kwargs.get('config_file','config.ini')
        self.configSectionName = kwargs.get('config_section','DEFAULT')
        # strip out our special options
        if 'product_name' in kwargs:
            del kwargs['product_name']
        if 'config_file' in kwargs:
            del kwargs['config_file']
        if 'config_section' in kwargs:
            del kwargs['config_section']
        # build a search path for INI files...
        configFiles = [ ]
        # look for INI files in $PRODUCTNAME_DIR/etc if a product_name is
        # provided and a corresponding $PRODUCTNAME_DIR envvar is defined
        if productName:
            productPath = os.getenv(productName.upper()+'_DIR')
            if productPath:
                configFiles.append(os.path.join(productPath,'etc',configFileName))
        # also search the current working dir if it is different from the above
        if not configFiles or (os.path.abspath(os.getcwd()) !=
            os.path.abspath(os.path.join(productPath,'etc'))):
            configFiles.append(os.path.join(os.getcwd(),configFileName))
        # read all available INI config options
        self.configParser = ConfigParser.SafeConfigParser()
        self.foundFiles = self.configParser.read(configFiles)
        self.configDefaults = { }
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
                defaultValue = getter(self.configSectionName,optionName)
                kwargs['default'] = defaultValue
                self.configDefaults[optionName] = defaultValue
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
        # do the normal option processing
        return optparse.OptionParser.add_option(self,*args,**kwargs)
        
    def parse_args(self,args=None,values=None,
        passphrase=None,prompt='Enter the pass phrase: '):
        """
        Parses command line arguments
        """
        # do the normal parsing
        (options,args) = optparse.OptionParser.parse_args(self,args,values)
        # build a table summarizing all non-secret options
        table = [ ]
        for opt in self.option_list:
            if opt.dest and opt.dest not in self.secretOptions:
                value = getattr(options,opt.dest)
                if opt.default == optparse.NO_DEFAULT:
                    default_value = '(none)'
                else:
                    default_value = opt.default
                table.append((str(opt),value,default_value,opt.help))
        # add the summary table to the returned options
        options._summary_table = table
        # is there is any secret data that needs decrypting?
        if self.secretOptions:
            import getpass
            # only introduce these external dependencies if necessary
            try:
                from Crypto.Hash import MD5 as hasher
                from Crypto.Cipher import AES as cipher
            except ImportError:
                raise 'secret options require the Crypto package'
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
    
    def get_config_info(self):
        """
        Returns a multi-line string describing our config setup
        """
        text = 'Runtime configuration defaults provided by the following files:\n\n  '
        text += '\n  '.join(self.foundFiles)
        text += '\n\nDefault values are:\n\n'
        for opt in self.option_list:
            if opt.dest:
                if opt.default == optparse.NO_DEFAULT:
                    default_value = '(none)'
                else:
                    default_value = opt.default
                text += '  %20s: %s\n' % (str(opt),default_value)
        return text
    
    def print_help(self,*args,**kwargs):
        """
        Appends config info to the standard OptionParser help
        """
        retval = optparse.OptionParser.print_help(self,*args,**kwargs)
        print '\n' + self.get_config_info()
        return retval
    
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