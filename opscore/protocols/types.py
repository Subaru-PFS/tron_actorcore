"""
Type declarations for SDSS-3 keyword values

Refer to https://trac.sdss3.org/wiki/Ops/Types
"""

# Created 30-Oct-2008 by David Kirkby (dkirkby@uci.edu)

import re
import textwrap

import opscore.utility.html as html

class ValueTypeError(Exception):
	pass
	
class InvalidValueError(Exception):
	"""
	Signals that an invalid value was detected during conversion from a string
	"""
	pass

class Descriptive(object):
	"""
	A self-describing type mixin
	
	Types that use this mixin are responsible for filling an array cls.descriptors
	of tuples (label,value) in their constructor.
	"""
	def describe(self):
		"""
		Returns a plain-text multi-line description
		
		The final newline character is omitted so that the return value
		can be printed. The description is indented with spaces assuming
		a fixed-width font.
		"""
		text = ''
		for label,value in self.descriptors:
			text += '%12s: %s\n' % (label,value)
		return text[:-1]	

	def describeAsHTML(self):
		"""
		Returns a description packaged as an HTML fragment
		
		Uses the following CSS elements: div.vtype, div.descriptor,
		span.label, span.value
		"""
		content = html.Div(className='vtype')
		for label,value in self.descriptors:
			content.append(html.Div(
				html.Span(label,className='label'),
				html.Span(value,className='value'),
				className='descriptor'
			))
		return content

class ValueType(type,Descriptive):
	"""
	A metaclass for types that represent an enumerated or numeric value
	"""
	def __new__(cls,*args,**kwargs):
		"""
		Allocates memory for a new ValueType class
		"""
		def doRepr(self):
			if self.units:
				units = ' ' + self.units
			else:
				units = ''
			if self.reprFmt:
				return '%s(%s%s)' % (cls.__name__,self.reprFmt % self,units)
			else:
				return '%s(%s%s)' % (cls.__name__,cls.baseType.__repr__(self),units)
				
		def doStr(self):
			if self.strFmt:
				return self.strFmt % self
			elif self.reprFmt:
				return self.reprFmt % self
			else:
				return cls.baseType.__str__(self)
				
		get = lambda name,default=None: kwargs.get(name,cls.__dict__.get(name,default))

		dct = {
			'reprFmt': get('reprFmt'),
			'strFmt': get('strFmt'),
			'invalid': get('invalid'),
			'units': get('units'),
			'help': get('help'),
			'name': get('name'),
			'__repr__': doRepr,
			'__str__': doStr
		}
		if hasattr(cls,'new'):
			dct['__new__'] = cls.new
		if hasattr(cls,'init'):
			cls.init(dct,*args,**kwargs)
		return type.__new__(cls,cls.__name__,(cls.baseType,),dct)

	def addDescriptors(cls):
		if cls.units:
			cls.descriptors.append(('Units',cls.units))

	def __init__(cls,*args,**kwargs):
		"""
		Initializes a new ValueType class
		"""
		super(ValueType,cls).__init__(cls.__name__,(cls.baseType,),{})
		cls.descriptors = [ ]
		if cls.help:
			pad = '\n' + ' '*14
			formatted = textwrap.fill(textwrap.dedent(cls.help),width=62).replace('\n',pad)
			cls.descriptors.append(('Description',cls.help))
		cls.descriptors.append(('Type','%s (%s,%s)' %
			(cls.__name__,cls.baseType.__name__,cls.storage)))
		cls.addDescriptors()
		if cls.invalid:
			cls.descriptors.append(('Invalid',cls.invalid))

	def __mul__(self,amount):
		if isinstance(amount,int):
			return RepeatedValueType(self,amount,amount)
		elif not isinstance(amount,tuple):
			raise TypeError('Cannot multiply ValueType by type %r' % type(amount))
		if len(amount) == 0 or len(amount) > 2:
			raise ValueTypeError('Repetions should be specified as *(min,) or *(min,max)')
		minRepeat = amount[0]
		if len(amount) == 2:
			maxRepeat = amount[1]
		else:
			maxRepeat = None
		return RepeatedValueType(self,minRepeat,maxRepeat)
	
	def __repr__(self):
		return self.__name__

	def validate(self,value):
		if self.invalid and value == self.invalid:
			raise InvalidValueError
		return value

class RepeatedValueType(Descriptive):

	def __init__(self,vtype,minRepeat,maxRepeat):
		if not isinstance(vtype,ValueType):
			raise ValueTypeError('RepeatedValueType only works for a ValueType')
		self.vtype = vtype
		if not isinstance(minRepeat,int) or (maxRepeat and not isinstance(maxRepeat,int)):
			raise ValueTypeError('Expected integer min/max repetitions for RepeatedValueType')
		self.minRepeat = minRepeat
		self.maxRepeat = maxRepeat
		if self.minRepeat < 0 or (self.maxRepeat and self.maxRepeat < self.minRepeat):
			raise ValueTypeError('Expected min <= max for RepeatedValueType')
		if self.minRepeat == 1:
			times = 'once'
		else:
			times = '%d times' % self.minRepeat
		if self.minRepeat == self.maxRepeat:
			repeatText = times
		elif self.maxRepeat is None:
			repeatText = 'at least ' + times
		else:
			repeatText = '%d-%d times' % (self.minRepeat,self.maxRepeat)
		self.descriptors = [('Repeated',repeatText)]
		self.descriptors.extend(self.vtype.descriptors)
			
	def __repr__(self):
		if self.minRepeat == self.maxRepeat:
			return '%r*%d' % (self.vtype,self.minRepeat)
		elif self.maxRepeat is None:
			return '%r*(%d,)' % (self.vtype,self.minRepeat)
		else:
			return '%r*(%d,%d)' % (self.vtype,self.minRepeat,self.maxRepeat)


class Invalid(object):
	units = ''
	def __repr__(self):
		return '(invalid)'
	
# a constant object representing an invalid value
InvalidValue = Invalid()

class Float(ValueType):
	baseType = float
	storage = 'flt4'
	def new(cls,value):
		return float.__new__(cls,cls.validate(value))
	
class Double(ValueType):
	baseType = float
	storage = 'flt8'
	def new(cls,value):
		return float.__new__(cls,cls.validate(value))

class Int(ValueType):
	baseType = int
	storage = 'int4'
	def new(cls,value):
		return int.__new__(cls,cls.validate(value))

class Long(ValueType):
	baseType = long
	storage = 'int8'
	def new(cls,value):
		return long.__new__(cls,cls.validate(value))

class String(ValueType):
	baseType = str
	storage = 'text'
	def new(cls,value):
		return str.__new__(cls,cls.validate(value))
#	@classmethod
#	def init(cls,dct,*args,**kwargs):
#		def sql(self):
#			# enclose in single quotes and escape any embedded single quotes as ''
#			return "'%s'" % self.replace("'","''")
#		dct['sqlValue'] = sql

class Filename(String):
	pass

class UInt(ValueType):
	baseType = long
	storage = 'int4'
	def new(cls,value):
		lvalue = long(cls.validate(value))
		if lvalue < 0 or lvalue > 0xffffffff:
			raise ValueError('Invalid literal for UInt: %r' % value)
		return long.__new__(cls,value)
#	@classmethod
#	def init(cls,dct,*args,**kwargs):
#		# repr(long(0)) is 0L so use str() to suppress the 'L'
#		dct['sqlValue'] = long.__str__


class Hex(UInt):
	reprFmt = '0x%x'
	def new(cls,value):
		try:
			return long.__new__(cls,cls.validate(value),16)
		except TypeError:
			return UInt.new(cls,value)

# Enumerated value type
class Enum(ValueType):

	baseType = int
	storage = 'int2'
	
	@classmethod
	def init(cls,dct,*args,**kwargs):
		if not args:
			raise ValueTypeError('missing enum labels in ctor')
		dct['enumLabels'] = args
		dct['enumValues'] = dict(zip(args,range(len(args))))
		def doStr(self):
			return self.enumLabels[self]
		dct['__str__'] = doStr
		if dct['strFmt']:
			print 'Enum: ignoring strFmt metadata'

	def new(cls,value):
		"""
		Initializes a new enumerated instance
		
		Value can either be an integer index or else a recognized label.
		"""
		cls.validate(value)
		if isinstance(value,int):
			if value >= 0 and value < len(cls.enumLabels):
				return int.__new__(cls,value)
			else:
				raise ValueError('Invalid index for Enum: %d' % value)
		try:
			return int.__new__(cls,cls.enumValues[value])
		except KeyError:
			raise ValueError('Invalid label for Enum: "%s"' % value)
			
	def addDescriptors(cls):
		cls.descriptors.append(('Values',','.join(cls.enumLabels)))

# Boolean value type
class Bool(ValueType):
	
	baseType = int # bool cannot be subclassed
	storage = 'int2'
	
	@classmethod
	def init(cls,dct,*args,**kwargs):
		if not args or not len(args) == 2:
			raise ValueTypeError('missing true/false labels in ctor')
		dct['falseValue'] = args[0]
		dct['trueValue'] = args[1]
		def doStr(self):
			if self:
				return self.trueValue
			else:
				return self.falseValue
		dct['__str__'] = doStr
		if dct['strFmt']:
			print 'Bool: ignoring strFmt metadata'
	
	def new(cls,value):
		"""
		Initializes a new boolean instance
		
		Value must be one of the true/false labels or else a True/False literal
		"""
		cls.validate(value)
		if value is True or value == cls.trueValue:
			return int.__new__(cls,True)
		elif value is False or value == cls.falseValue:
			return int.__new__(cls,False)
		else:
			raise ValueError('Invalid Bool value: %r' % value)

	def addDescriptors(cls):
		cls.descriptors.append(('False',cls.falseValue))
		cls.descriptors.append(('True',cls.trueValue))

# Bitfield value type
class Bits(ValueType):
	
	baseType = long
	storage = 'int4'
	
	fieldSpec = re.compile('([a-zA-Z0-9_]+)?(?::([0-9]+))?$')
	
	@staticmethod
	def binary(value,width):
		return ''.join([str((value>>shift)&1) for shift in range(width-1,-1,-1)])

	@classmethod
	def init(cls,dct,*args,**kwargs):
		if not args:
			raise ValueTypeError('missing bitfield specs in ctor')
		offset = 0
		fields = { }
		specs = [ ]
		for field in args:
			parsed = cls.fieldSpec.match(field)
			if not parsed:
				raise ValueTypeError('invalid bitfield spec: %s' % field)
			(name,width) = parsed.groups()
			width = int(width or 1)
			if name:
				specs.append((name,width))
				fields[name] = (offset,long((1<<width)-1))
			offset += width
			if offset > 32:
				raise ValueTypeError('total bitfield length > 32')
		dct['width'] = offset
		dct['fieldSpecs'] = specs
		dct['bitFields'] = fields
		def getAttr(self,name):
			if name not in self.bitFields:
				raise AttributeError('no such bitfield "%s"' % name)
			(offset,mask) = self.bitFields[name]
			return (self >> offset) & mask
		dct['__getattr__'] = getAttr
		def setAttr(self,name,value):
			if name not in self.bitFields:
				raise AttributeError('no such bitfield "%s"' % name)
			(offset,mask) = self.bitFields[name]
			return self.__class__((self & ~(mask << offset)) | ((value & mask) << offset))
		dct['set'] = setAttr
		def doStr(self):
			return '(%s)' % ','.join(
				['%s=%s' % (n,Bits.binary(getAttr(self,n),w)) for (n,w) in self.fieldSpecs]
			)
		dct['__str__'] = doStr
		if dct['strFmt']:
			print 'Bits: ignoring strFmt metadata'
		
	def new(cls,value):
		return long.__new__(cls,cls.validate(value))

	def addDescriptors(cls):
		for index,(name,width) in enumerate(cls.fieldSpecs):
			offset,mask = cls.bitFields[name]
			shifted = Bits.binary(mask << offset,cls.width)
			cls.descriptors.append(('Field-%d' % index,'%s %s' % (shifted,name)))
