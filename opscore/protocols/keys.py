"""
Core classes for SDSS-3 keyword validation

Refer to https://trac.sdss3.org/wiki/Ops/Validation for details.
"""

# Created 18-Nov-2008 by David Kirkby (dkirkby@uci.edu)

import textwrap
import imp
import sys
import hashlib

import RO.AddCallback

import opscore.protocols.types as protoTypes
import opscore.protocols.messages as protoMess
import opscore.utility.html as utilHtml

class KeysError(Exception):
	pass

class Consumer(object):
	"""
	Consumes parsed messages
	"""
	debug = False
	indent = 0

	def trace(self,what):
		if self.debug:
			print '%s%r << %r' % (' '*Consumer.indent,self,what)
			Consumer.indent += 1
			
	def passed(self,what):
		if self.debug:
			Consumer.indent -= 1
			print '%sPASS >> %r' % (' '*Consumer.indent,what)
		return True

	def failed(self,reason):
		if self.debug:
			Consumer.indent -= 1
			print '%sFAIL: %s' % (' '*Consumer.indent,reason)
		return False

	def consume(self,what):
		raise NotImplementedError

class TypedValues(Consumer):
	"""
	Consumes typed command or keyword values
	"""
	def __init__(self,vtypes):
		self.vtypes = [ ]
		self.minVals = 0
		self.maxVals = 0
		for vtype in vtypes:
			if isinstance(vtype,protoTypes.RepeatedValueType):
				if vtype.minRepeat != vtype.maxRepeat and vtype is not vtypes[-1]:
					raise KeysError('Repetition range only allowed for last value type')
				self.minVals += vtype.minRepeat
				if self.maxVals is not None and vtype.maxRepeat is not None:
					self.maxVals += vtype.maxRepeat
				else:
					self.maxVals = None
				self.vtypes.append(vtype)
			elif isinstance(vtype,protoTypes.ValueType):
				self.minVals += 1
				self.maxVals += 1
				self.vtypes.append(protoTypes.RepeatedValueType(vtype,1,1))
			else:
				raise KeysError('Invalid value type: %r' % vtype)
		if self.maxVals == 0:
			self.descriptor = 'none'
		elif self.maxVals == self.minVals:
			self.descriptor = self.minVals
		elif self.maxVals is None:
			self.descriptor = '%d or more' % self.minVals
		else:
			self.descriptor = '%d-%d' % (self.minVals,self.maxVals)
		

	def __repr__(self):
		return 'Types%r' % self.vtypes

	def consume(self,values):
		self.trace(values)
		# remember the original values in case we need to restore them later
		self.checkpoint = protoMess.Values(values)
		# try to convert each keyword to its expected type
		self.index = 0
		for repeat in self.vtypes:
			vtype = repeat.vtype
			offset = 0
			while offset < repeat.minRepeat:
				if not self.consumeNextValue(vtype,values):
					values[:] = self.checkpoint
					return self.failed("expected value type %r" % vtype)
				offset += 1
			while repeat.maxRepeat is None or offset < repeat.maxRepeat:
				if not self.consumeNextValue(vtype,values):
					break
				offset += 1
		if self.index != len(values):
			values[:] = self.checkpoint
			return self.failed("not all values consumed: %s" % values[self.index:])
		return self.passed(values)

	def consumeNextValue(self,valueType,values):
		try:
			string = values[self.index]
			try:
				values[self.index] = valueType(string)
			except protoTypes.InvalidValueError:
				values[self.index] = protoTypes.InvalidValue
			self.index += 1
			return True
		except (IndexError,ValueError,TypeError):
			return False
	
	def describeAsHTML(self):
		content = utilHtml.Div(
			utilHtml.Div(
				utilHtml.Span('Values',className='label'),
				utilHtml.Span(self.descriptor,className='value'),
				className='descriptor'
			),
			className='vtypes'
		)
		for vtype in self.vtypes:
			content.extend(vtype.describeAsHTML().children)
		return content
	
	def describe(self):
		text = '%12s: %s\n' % ('Values',self.descriptor)
		for vtype in self.vtypes:
			extra = vtype.describe().replace('\n','\n    ')
			text += '\n    %s\n' % extra
		return text

class Key(Consumer):
	"""
	Base class for a command or reply keyword consumer
	"""
	def __init__(self,name,*vtypes,**metadata):
		self.name = name
		self.typedValues = TypedValues(vtypes)
		self.help = metadata.get('help',None)
		if 'unique' in metadata:
			self.unique = metadata.get('unique')

	def __repr__(self):
		return 'Key(%s)' % self.name
		
	def consume(self,keyword):
		self.trace(keyword)
		if keyword.name != self.name:
			return self.failed('keyword has wrong name')
		if not self.typedValues.consume(keyword.values):
			return self.failed('no match for keyword values')
		keyword.matched = True
		return self.passed(keyword)

	def create(self,*values):
		"""
		Returns a new Keyword using this Key as a template
		
		Any keyword values must match the expected types or else this
		method returns None. The returned keyword will have typed
		values.
		"""
		if len(values) == 1 and isinstance(values[0],list):
			values = values[0]
		keyword = protoMess.Keyword(self.name,values)
		if not self.consume(keyword):
			raise KeysError('value types do not match for keyword %s: %r'
				% (self.name,values))
		return keyword
	
	def describeAsHTML(self):
		content = utilHtml.Div(
			utilHtml.Div(self.name,className='keyname'),
			
			className='key'
		)
		desc = utilHtml.Div(className='keydesc')
		content.append(desc)
		if self.help:
			desc.append(utilHtml.Div(
				utilHtml.Span('Description',className='label'),
				utilHtml.Span(self.help,className='value'),
				className='descriptor'
			))
		desc.extend(self.typedValues.describeAsHTML().children)
		return content		
	
	def describe(self):
		text = '%12s: %s\n' % ('Keyword',self.name)
		if self.help:
			pad = '\n' + ' '*14
			formatted = textwrap.fill(textwrap.dedent(self.help),width=66).replace('\n',pad)
			text += '%12s: %s\n' % ('Description',formatted)
		text += self.typedValues.describe()
		return text

class KeysManager(object):

	keys = [ ]

	# These need to be classmethods, not static methods, so that
	# subclasses each access their own keys list.
	@classmethod
	def setKeys(cls,kdict):
		cls.keys = [ ]
		cls.addKeys(kdict)
		
	@classmethod
	def addKeys(cls,kdict):
		if not isinstance(kdict,KeysDictionary):
			raise KeysError('Cmd keys must be provided as a KeysDictionary')
		cls.keys.append(kdict)
		
	@classmethod
	def getKey(cls,name):
		for kdict in cls.keys:
			if name in kdict:
				return kdict[name]
		raise KeysError('No such registered keyword <%s>' % name)
		
class CmdKey(Consumer,KeysManager):
	"""
	Consumes a command keyword
	"""
	def __init__(self,key):
		self.key = key
	
	def __repr__(self):
		return 'CmdKey(%s)' % self.key.name
	
	def consume(self,where):
		self.trace(where)
		keyword = where.keyword()
		if not keyword:
			return self.failed('no keywords available to consume')
		if not self.key.consume(keyword):
			return self.failed('no match for command keyword')
		where.advance()
		return self.passed(where)

class KeysDictionaryError(KeysError):
	pass

class KeysDictionary(object):
    """A collection of Keys associated with a given name (typically the name of an actor).
    
    Note: contains a registry of all known KeysDictionaries, for use by the load method.
    """
	registry = { }

	def __init__(self,name,version,*keys):
		"""
		Creates a new named keys dictionary
		
		Overwrites any existing dictionary with the same name. The
		version should be specified as a (major,minor) tuple of
		integers.
		"""
		self.name = name
		try:
			(major,minor) = map(int,version)
		except (ValueError,TypeError):
			raise KeysDictionaryError(
			'Invalid version: expected (major,minor) tuple of integers, got %r' % version)
		self.version = version
		KeysDictionary.registry[name] = self
		self.keys = { }
		for key in keys:
			self.add(key)

	def add(self,key):
		"""
		Adds a key to the dictionary

		By default, the key is registered under key.name but this can be
		overridden by setting a key.unique attribute. Attempting to
		add a key using a name that is already assigned raises an
		exception.
		"""
		if not isinstance(key,Key):
			raise KeysDictionaryError('KeysDictionary can only contain Keys')
		name = getattr(key,'unique',key.name)
		if name in self.keys:
			raise KeysDictionaryError('KeysDictionary name is not unique: %s' % name)
		self.keys[name] = key

	def __getitem__(self,name):
		return self.keys[name]

	def __contains__(self,name):
		return name in self.keys
		
	def describe(self):
		text = 'Keys Dictionary for "%s" version %r\n' % (self.name,self.version)
		for name in sorted(self.keys):
			text += '\n' + self.keys[name].describe()
		return text
		
	def describeAsHTML(self):
		content = utilHtml.Div(
			utilHtml.Div(
				utilHtml.Span(self.name,className='actorName'),
				utilHtml.Span('%d.%d' % self.version,className='actorVersion'),
				className='actorHeader'
			),
			className='actor'
		)
		for name in sorted(self.keys):
			content.append(self.keys[name].describeAsHTML())
		return content

	@staticmethod
	def load(dictname,forceReload=False):
		"""
		Loads a KeysDictionary by name, returning the result
		
		Uses an in-memory copy, if one is available, otherwise loads the
		dictionary from disk. Use forceReload to force the dictionary to
		be loaded from disk even if it is already in memory. Raises a
		KeysDictionaryError if a dictionary cannot be found for dictname.
		"""
		if not forceReload and dictname in KeysDictionary.registry:
			return KeysDictionary.registry[dictname]
		# try to find a corresponding file on the import search path
		dictfile = None
		try:
			# get the path corresponding to the actorkeys package
			import actorkeys
			keyspath = sys.modules['actorkeys'].__path__
			# open the file corresponding to the requested keys dictionary
			(dictfile,name,description) = imp.find_module(dictname,keyspath)
			# create a global symbol table for evaluating the keys dictionary expression
			symbols = {
				'__builtins__': __builtins__,
				'Key': Key,
				'KeysDictionary': KeysDictionary
			}
			for (name,value) in protoTypes.__dict__.iteritems():
				if isinstance(value,type) and issubclass(value,protoTypes.ValueType):
					symbols[name] = value
			# evaluate the keys dictionary as a python expression
			filedata = dictfile.read()
			kdict = eval(filedata,symbols)
			# do a checksum so that we can detect changes independently of versioning
			kdict.checksum = hashlib.md5(filedata).hexdigest()
			return kdict
		except ImportError:
			raise KeysDictionaryError('no keys dictionary found for %s' % dictname)
		except SyntaxError:
			raise KeysDictionaryError('badly formatted keys dictionary in %s'
				% dictfile.name)
		finally:
			if dictfile: dictfile.close()
