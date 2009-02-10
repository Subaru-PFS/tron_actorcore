"""
Type declarations for SDSS-3 keyword values

Refer to https://trac.sdss3.org/wiki/Ops/Types
"""

# Created 30-Oct-2008 by David Kirkby (dkirkby@uci.edu)

import re
import textwrap

import ops.core.utility.html as html

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
			'sqlValue': cls.baseType.__repr__,
			'binLength': getattr(cls,'binLength',None),
			'binFormat': getattr(cls,'binFormat',None),
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
		cls.descriptors.append(('Type','%s (%s)' % (cls.__name__,cls.baseType.__name__)))
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
	sqlType = 'real'
	def new(cls,value):
		return float.__new__(cls,cls.validate(value))
	
class Double(ValueType):
	baseType = float
	sqlType = 'double precision'
	binLength = 8
	binFormat = 'd'
	def new(cls,value):
		return float.__new__(cls,cls.validate(value))

class Int(ValueType):
	baseType = int
	sqlType = 'integer'
	def new(cls,value):
		return int.__new__(cls,cls.validate(value))
	
class String(ValueType):
	baseType = str
	sqlType = 'text'
	binFormat = 's'
	def new(cls,value):
		return str.__new__(cls,cls.validate(value))
	@classmethod
	def init(cls,dct,*args,**kwargs):
		def sql(self):
			# enclose in single quotes and escape any embedded single quotes as ''
			return "'%s'" % self.replace("'","''")
		dct['sqlValue'] = sql

class Filename(String):
	pass

class UInt(ValueType):
	baseType = long
	sqlType = 'integer'
	binLength = 4
	binFormat = 'i'
	def new(cls,value):
		lvalue = long(cls.validate(value))
		if lvalue < 0 or lvalue > 0xffffffff:
			raise ValueError('Invalid literal for UInt: %r' % value)
		return long.__new__(cls,value)
	@classmethod
	def init(cls,dct,*args,**kwargs):
		# repr(long(0)) is 0L so use str() to suppress the 'L'
		dct['sqlValue'] = long.__str__


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
	sqlType = 'smallint'
	binLength = 2
	binFormat = 'h'
	
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
	sqlType = 'smallint'
	
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
	sqlType = 'integer'
	
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

import unittest

class TypesTest(unittest.TestCase):
	
	def test00(self):
		"Types created with valid string values"
		self.assertEqual(Float()('1.23'),1.23)
		self.assertEqual(Int()('-123'),-123)
		self.assertEqual(String()('hello, world'),'hello, world')
		self.assertEqual(UInt()('+123'),123)
		self.assertEqual(Hex()('123'),0x123)

	def test01(self):
		"Types created with valid non-string values"
		self.assertEqual(Float()(1.23),1.23)
		self.assertEqual(Int()(-123),-123)
		self.assertEqual(Int()(-12.3),-12)
		self.assertEqual(String()(+123),'123')
		self.assertEqual(UInt()(+123),123)
		self.assertEqual(Hex()(0x123),0x123)
		self.assertEqual(Hex()(123),123)

	def test02(self):
		"Types created with invalid string values"
		self.assertRaises(ValueError,lambda: Float()('1.2*3'))
		self.assertRaises(ValueError,lambda: Int()('1.2'))
		self.assertRaises(ValueError,lambda: UInt()('-123'))
		self.assertRaises(ValueError,lambda: Hex()('xyz'))

	def test03(self):
		"Types created with invalid non-string values"
		self.assertRaises(ValueError,lambda: String()(u'\u1234'))
		self.assertRaises(ValueError,lambda: UInt()(-123))
		self.assertRaises(ValueError,lambda: Hex()(-123))
		
	def test04(self):
		"Enumeration created with valid values"
		COLOR = Enum('red','green','blue')
		self.assertEqual(COLOR('green'),1)
		self.assertEqual(str(COLOR('red')),'red')
		self.assertEqual(COLOR(2),2)
		self.assertEqual(str(COLOR(2)),'blue')
		
	def test05(self):
		"Enumeration created with invalid values"
		COLOR = Enum('red','green','blue')
		self.assertRaises(ValueError,lambda: COLOR('brown'))
		self.assertRaises(ValueError,lambda: COLOR('0'))
		self.assertRaises(ValueError,lambda: COLOR(3))
		self.assertRaises(ValueError,lambda: COLOR(-1))
		
	def test06(self):
		"Bitfield created with valid values"
		REG = Bits('addr:8',':1','strobe')
		r1 = REG(0x27f)
		self.assertEqual(r1.addr,0x7f)
		self.assertEqual(r1.strobe,1)
		r2 = REG(0).set('strobe',1).set('addr',0x7f)
		self.assertEqual(r2,0x27f)
		self.assertEqual(str(r2),'(addr=01111111,strobe=1)')
		
	def test07(self):
		"Invalid bitfield ctor"
		self.assertRaises(ValueTypeError,lambda: Bits())
		self.assertRaises(ValueTypeError,lambda: Bits('addr*:2'))
		self.assertRaises(ValueTypeError,lambda: Bits('addr:-2'))
		self.assertRaises(ValueTypeError,lambda: Bits('addr:99'))
		
	def test08(self):
		"Bool created with valid values"
		B = Bool('Nay','Yay')
		self.failUnless(B('Yay') == True)
		self.failUnless(B('Nay') == False)
		self.failUnless(B(False) == False)
		self.failUnless(B(True) == True)
		self.assertEqual(str(B(False)),'Nay')
		self.assertEqual(str(B(True)),'Yay')
		
	def test09(self):
		"Repeated value types with valid ctor"
		vec1 = RepeatedValueType(Float(),3,3)
		vec2 = RepeatedValueType(Float(),0,3)
		vec3 = Float()*3
		vec4 = Float()*(0,3)
		vec5 = Float()*(1,)
		
	def test10(self):
		"Repeated value types with invalid ctor"
		self.assertRaises(ValueTypeError,lambda: RepeatedValueType(Float(),'1','2'))
		self.assertRaises(ValueTypeError,lambda: RepeatedValueType(Float(),1.0,2.1))
		self.assertRaises(ValueTypeError,lambda: RepeatedValueType(Float(),2,1))
		self.assertRaises(ValueTypeError,lambda: RepeatedValueType(Float(),-1,1))
		self.assertRaises(ValueTypeError,lambda: RepeatedValueType(Float(),1,'abc'))
		
	def test11(self):
		"Values initialized with an invalid string literal"
		self.assertRaises(InvalidValueError,lambda: Float(invalid='???')('???'))
		self.assertRaises(InvalidValueError,lambda: Enum('RED','GREEN','BLUE',invalid='PINK')('PINK'))
		self.assertRaises(InvalidValueError,lambda: UInt(invalid='-')('-'))

if __name__ == '__main__':
	#F = Float(units='C',strFmt='%.2f',help='Air Temperature',invalid='?')*1
	#E = Enum('RED','GREEN','BLUE',help='Colors',invalid='PINK')*(1,3)
	#print F.describeAsHTML()
	#print E.describe()
	unittest.main()
