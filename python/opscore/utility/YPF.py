""" ypf.py -- read/write Yanny Parameter Files.
    See http://www.sdss.org/dr5/dm/flatFiles/yanny.html.

y = YPF()
  or
y = YPF(filename)
  or
y = YPF(filename, template=True)

Get variables:
mjd = y.vars['mjd'].value

Get structures as numpy recarrays:
weather = y.structs['weather'].asArray()
wet = n.where(weather['humidity'] > 80.0)

Get structures as lists of python objects:
weather = y.structs['weather'].asObjlist()
wet = [w for w in weather if w.humidity > 80.0]

#y.structs['newstruct'] = numpy_structured_array
#y.enums['newenum'] = list of strings
#y.vars['newvar'] = value

# Generate a new .par file or a string:
s = y.asString()
y.writefile('filename')


"""
from __future__ import print_function

import logging
import numpy as np
import os
import re
import time

import pdb
from functools import reduce

__version__ = '2.2'
__all__ = ['YPF', 'readOneStruct']

# Regexps for all the various data types.
floatRe_s = r'''([+-]?
                 (?:(?: \d+\.\d*) | (?: \d*\.\d+) | (?:\d+))
                 (?:[eE][-+]?\d+)?) # float
                 '''
intRe_s = r'''([+-]?\d+) # int
'''

# Strings can either be double-quoted or unquoted.
# Or, apparently, potentially nested {}s, which we simply can't deal with in regexps.
#   [ That re for the {} strings is leftover from something based on an incorrect anecdotal description.  ]
#
charsDQRe_s = r'(?:(?:\\["\\])|[^"\\])'   # \\ or \" or any non-escaped character.
charsNQRe_s = r'(?:(?:\\["\\ 	])|[!-~])' # \\ or \space or any non-escaped character.
strRe_s = r'''((?:"%s*") |
               (?:%s+)   |
               (?:{ (?:[^{]*{[^}]*}) *} *})) # string
              ''' \
    % (charsDQRe_s, charsNQRe_s)

# ctype -> regexp
typeRe_s = { 'int':intRe_s, \
             'short':intRe_s, \
             'float':floatRe_s, \
             'double':floatRe_s, \
             'char':strRe_s
             }

# ctype -> numpy type
nptypes = { 'int':'i4',
            'short':'i2',
            'float':'f4',
            'double':'f8',
            'char':'S'
            }

class YPFVar(object):
    """  A Yanny par file keyword/value pair.
    
        A line may contain a keyword/value pair. The first token is the keyword. The remainder
        of the line, up to a comment-delimiting '#' sign, is the value of the keyword. In the
        following example, the value '51256' is assigned to the keyword 'MJD', and the
        value 'u g r i z' is assigned to the keyword 'filters':

        mjd	51256		# MJD day number of observations
        filters u g r i z

    Notes:
        I take it there is no data typing for the variables, including arrays and strings with '#'
        signs in them. Still, try to convert single floats/ints to the given type. It may be wrong
        to try, though.

        Values can be blank..
    """
    re = re.compile(r'''\s*([a-z_][a-z_0-9]*)
                        \s+
                        (?:%s|%s|([^#]*?))\s*(?:\#(.*))?
                        $''' % (floatRe_s, intRe_s), re.VERBOSE|re.IGNORECASE)
    
    def __init__(self,
                 fromString=None,
                 debug=0):

        self.name = None
        self.value = None
        self.comment = None
        
        if fromString:
            self.parse(fromString, debug=debug)
            
    def __str__(self):
        return "YPFVar(name=%s, value=%s, comment=%s)" % (self.name, self.value, self.comment)

    def asString(self):
        """ Return ourselves formatted for storage in a .par file"""

        if self.comment:
            comment = " # %s" % (self.comment)
        else:
            comment = ""
            
        return "%s %s%s" % (self.name, self.value, comment)

    def parse(self, s, debug=0):
        """ parse a single line as a yanny variable definition.

        Note:
            Tries to recognize ints and floats and convert them. This may be a mistake.
            
        Returns:
        Raises:
            RuntimeError on parsing failure.

        """
        
        m = self.re.match(s)
        if not m:
            logging.log(50, 'unparseable variable definition: %s' % (s))
            raise RuntimeError('parsing variable from: %s' %s)
        
        # _Try_ to convert to float/int. For "arrays", you get a string.
        # Take this out -- CPL.
        #
        name, fmatch, imatch, smatch, comment = m.groups()
        self.name = name
        if fmatch != None: self.value = float(fmatch)
        elif imatch != None: self.value = int(imatch)
        else: self.value = smatch

        if comment:
            self.comment = comment

        if debug > 5:
            print("Parsed YPFVar %s -> %s" % (s, self))
        
class YPFEnum(object):
    """ A Yanny par file enumerated data type.  

    These are similar to C-language enums, in that one lists a comma-separated
    number of identifying tags in a multi-line typedef statement which 
    are internally stored 
    as ints, 0, 1, 2, 3, etc, but which are referenced by the tag for 
    mnemonic convenience or
    documentation purposes.  The tags should start with a letter,
    and be alphanumeric, like a variable-name in C. 
    Underscores are permitted in the tag name, i.e.  START_RUN, END_RUN.
    
    By convention enum values (i.e. START,END) are all-caps and
    newlines separate the comma-separated entries in the typedef definition
    of the enum.
    [ "By convention"?!? Yes or no? Yes, I wager. -- CPL ]
    """

    tagRE = re.compile(r'''^\s*(?P<tag>[a-z][a-z0-9_]*)
                           \s*(?:,)?\s*(?P<rest>.*)''',
                       re.VERBOSE|re.IGNORECASE)
    typedefNameRE = re.compile(r'''^\s*\}\s*(?P<name>[a-z][a-z0-9_]*)\s*;?\s*''',
                               re.VERBOSE|re.IGNORECASE)

    def __init__(self, name, fields=None,
                 fromStream=None):

        self.name = name
        self.tags = []

        if fields:
            self.construct(fields)

    def construct(self, tags):
        self.tags = tags

    def maxlen(self):
        """ Return the longest tag name. """
        
        return reduce(max, map(len, self.tags))

    def re(self):
        """ Return a regexp which would match us. """

        return '(%s)' % ('|'.join(self.tags))
    
    def asString(self):
        """ Return ourselves formatted for storage in a .par file"""

        edefs = []
        edefs.append("typedef enum {")
        for t in self.tags:
            edefs.append('    %s;' % (t))
        edefs.append("} %s;" % (self.name));
        edefs.append('')
        
        return '\n'.join(edefs)

    @classmethod
    def parseDef(self, firstLine, lines):
        parts = []

        m = re.match(r'\s*typedef\s+enum\s*\{\s*(?P<rest>.*)', firstLine,
                     re.VERBOSE)
        if not m:
            raise RuntimeError('No prefix mathed in %s' % (firstLine))
        
        l = m.groupdict()['rest']
        nlines = 0
        parts = []
        while l != None:
            nlines += 1
            l = l.strip()

            # Finished with this line, fetch the next one.
            if l == '' or l.startswith('#'):
                l = next(lines)
                continue

            # Look for the ending enum name.
            m = self.typedefNameRE.match(l)
            if m:
                g = m.groupdict()
                name = g['name']
                return nlines, name, parts

            # Get the next tag
            m = self.tagRE.match(l)
            if not m:
                raise RuntimeError('unexpected end of enum: %s' % (l))
            g = m.groupdict()
            parts.append(g['tag'].upper())
                
            # Process the rest of the line.
            l = g['rest']
                         
class YPFStruct(object):

    # Match a single structure tag.
    typedefRE = re.compile(r'''\s*(?P<type>[a-z][a-z0-9_]*)
                            \s+
                            (?P<name>[a-z][a-z0-9_]*)
                            (?:\[(?P<arr1>(?:[1-9][0-9]*)?)\])?
                            (?:\[(?P<arr2>[1-9][0-9]*)\])?
                            ;\s*(?P<rest>.*)''',
                           re.VERBOSE|re.IGNORECASE)

    # The trailing semicolon is not in idReport's IDCOMMENT
    typedefNameRE = re.compile(r'''^\s*\}\s*(?P<name>[a-z][a-z0-9_]*)\s*;?\s*''',
                               re.VERBOSE|re.IGNORECASE)

    def __init__(self, name, enums=None, fields=None,
                 fromStream=None):

        self.name = name
        
        self.ctypes = []
        self.dtypes = []

        self.records = []
        self.strings = []

        if fields:
            self.construct(name, fields, enums)

    def __str__(self):
        fnames = [t[0] for t in self.ctypes]
        return "YPFStruct(name=%s, fields=%s)" % (self.name, fnames)

    @classmethod
    def parseDef(self, firstLine, lines):
        """ Parse a structure definition.

        Args:
            s    - a string which starts after the end of "typedef struct"

        Returns:
            - the part of s remaining after the structure definition has
              been consumed.
        """

        m = re.match(r'\s*typedef\s+struct\s*\{\s*(?P<rest>.*)', firstLine,
                     re.VERBOSE)
        if not m:
            raise RuntimeError('No prefix mathed in %s' % (firstLine))
        
        l = m.groupdict()['rest']

        nlines = 0
        parts = []
        while l != None:
            nlines += 1
            l = l.strip()

            # Finished with this line, fetch the next one.
            if l == '' or l.startswith('#'):
                l = next(lines)
                continue

            # Look for the ending struct name.
            m = self.typedefNameRE.match(l)
            if m:
                g = m.groupdict()
                name = g['name']
                return nlines, name, parts

            # Get the next column definition.
            m = self.typedefRE.match(l)
            if not m:
                raise RuntimeError('unmatched struct definition at %s' % (l))
            
            g = m.groupdict()
            if g['arr1']:
                arrSize = int(g['arr1'])
            else:
                arrSize = 1
                
            if g['arr2']:
                arrSize = (int(g['arr2']), arrSize)

            defn = (g['name'], g['type'], arrSize)
            parts.append(defn)

            # Process the rest of the line.
            l = g['rest']

    def defAsString(self):
        """ Return our definition, formatted for a .par file. """

        sdefs = []
        sdefs.append("typedef struct {")
        for f in self.ctypes:
            sdefs.append(self._fieldDefAsString(f))
            
        sdefs.append("} %s;" % (self.name))
        sdefs.append('')
        
        return '\n'.join(sdefs)

    def dataAsString(self):
        """ Return our data, formatted for a .par file. """

        # Force generation of .array
        d = self.asArray()
        slist = []
        for l in self.array:
            s = "%s %s" % (self.name, self.rowAsString(l))
            slist.append(s)
        return '\n'.join(slist)
            
    def _fieldDefAsString(self, fieldTuple):
        """ given a (name, type, len) tuple, return a Yanny struct field definition. """

        fname, ftype, flen = fieldTuple
        if ftype == 'char':
            # pdb.set_trace()
            try:
                slen, flen = flen
            except TypeError as e:
                slen = flen
                flen = 1

            if slen > 0:
                fname = '%s[%d]' % (fname, slen)
            else:
                fname = '%s[]' % (fname)
                
        if flen > 1: 
            return "    %s %s[%d];" % (ftype, fname, flen)
        else:
            return "    %s %s;" % (ftype, fname)

    def qstring(self, s):
        """ Return a string quoted properly for a Yanny structure. If the string
        does not need to be quoted, don't.
        """

        if '"' in s or ' ' in s or '\\' in s:
            return '"' + s.replace('\\', '\\\\').replace('"', '\\"') + '"'
        else:
            return s
    
    def rowAsString(self, r):
        """ Return a single row of our data, formatted for a .par file. """

        srec = []
        for i in range(len(self.ctypes)):
            cnv = str
            fname, ftype, flen = self.ctypes[i]
            if ftype == 'char':
                cnv = self.qstring
                try:
                    slen, flen = flen
                except TypeError as e:
                    slen = flen
                    flen = 1

            if flen > 1:
                s = '{' + ' '.join(map(cnv, r[i])) + '}'
            else:
                s = cnv(r[i])

            srec.append(s)

        return " ".join(srec)
    
    def construct(self, name, fields, enums):
        """ generate python and numpy type descriptors for the given field.

        Args:
            name        - the name of the structure.
            fields      - a list of (name, type, length) tuples defining the structure, where:
              name   - the name of the field
              type   - the YPF/C type name
              nelem  - The number of elements for this field. For string members, a pair
                       (string-length, nelem)
            enums    - dictionary of enum (names -> YPFEnum)

        Sets:
           .ctypes[]    - ftcl/C type. e.g. 'short', 'double'
           .dtypes[]    - numpy type. e.g. ('run', 'i4'), ('objtype', 'S10')
           .reParts[]   - the regexps for each field. 
           .reText      - the full, g.d-awful regexp.
           .re          - the compiled regexp
           .strings[]   - the indices of all string members.
           .strlens[]   - the max strlen for each column.
           
        Returns:
           Nothing
           
        """

        self.reParts = [name]
        rawfields = 0
        i = 0
        for f in fields:
            # pdb.set_trace()
            self.ctypes.append(f)
            fname, ftype, flen = f

            # Build this column's numpy dtype.
            if ftype in enums:
                enum = enums[ftype]
                nptype = 'S%d' % (enum.maxlen())
            else:
                nptype = nptypes[ftype]
            if ftype == 'char':
                try:
                    slen, flen = flen
                except TypeError as e:
                    slen = flen
                    flen = 1
                nptype = '%s%d' % (nptype, slen)
            dtype = (fname, nptype, flen)
            self.dtypes.append(dtype)

            # Build this column's regexp.
            if ftype in enums:
                enum = enums[ftype]
                thisre = enum.re()
            elif flen > 1:
                thisre = "\{\s*" + "\s+".join([typeRe_s[ftype]] * flen) + "\s*\}"
            else:
                thisre = typeRe_s[ftype]
            self.reParts.append(thisre)

            # We need to post-process matched strings; note which fields are strings.
            if ftype == 'char':
                self.strings.append(i)

            rawfields += flen
            i += 1
            
        # Glom together the regexp for the full row.
        self.reText = "^" + "\s+".join(self.reParts) + "\s*"
        self.re = re.compile(self.reText, re.VERBOSE|re.IGNORECASE)

        # The string lengths will be undefined until we have seen the data.
        self.strlens = np.zeros(rawfields, dtype='i4')
            
    def findParsingFailure(self, s):
        """ Figure out which field is not being parsed by consuming .reParts piece by piece. """

        rest = s
        matches = []
        for i in range(len(self.reParts)):
            thisre = '\s*' + self.reParts[i] + '(.*)'
            m = re.match(thisre, rest, re.VERBOSE|re.IGNORECASE)
            if not m:
                if i == 0:
                    dtype = self.name
                else:
                    dtype = self.dtypes[i-1][0]
                raise RuntimeError('Cannot parse field %d (%s) at: %s; previous matches: %s' % (i, dtype, rest, ';'.join(matches)))
            newRest = m.groups()[-1]
            matchedText = rest[:-len(newRest)]
            matches.append(matchedText)
            rest = newRest
        raise RuntimeError('Hunh? Failed to find parsing error in %s' % s)
            
    def parseOne(self, s):
        """ Parse what is expected to be a instance of our structure, and
            append the matches to .records. Until we have all the data, we
            keep the records in a python array of tuples.
        """
        
        m = self.re.match(s)
        if not m:
            self.findParsingFailure(s)
        mtuple = m.groups()

        # Strip douple quotes
        if self.strings:
            mlist = list(mtuple)
            for si in self.strings:
                s = mlist[si]
                if len(s) == 0: continue

                if s[0] == '"':
                    if s[-1] == '"':
                        s = s[1:-1]
                    else:
                        logging.log(50, 'missing end-quote in %s: %s' % (self.name, s,))
                        s = s[1:]
                    s = s.replace(r'\\\\', r'\\').replace(r'\"', r'"') 
                    mlist[si] = s
                    
                # Strange lesson mentioned in idlutils, and seen at least in idReport files.
                if len(s) > 0 and s[0] == '{':         
                    if re.search("^{ *{ *} *}\s*$", s):
                        mlist[si] = ""
            mtuple = tuple(mlist)
            
        # It actually pays to know the length of the longest string, for .seal()
        strlens = map(len,mtuple)
        self.strlens = np.array([self.strlens, strlens]).max(axis=0)        
            
        self.records.append(mtuple)

    def fixupS0s(self, a):
        """ Assign lengths to all the unsized char[] columns using the
            .strlens field we calculated while reading the data in. """

        in_i = 0                        # Any input arrays have been flattened.
        for i in range(len(self.dtypes)):
            fname, dtype, nelem = self.dtypes[i]
            if dtype != 'S0':
                in_i += nelem
                continue

            # For arrays of strings, make the lengths uniform
            maxlen = np.max(self.strlens[in_i:in_i+nelem])
            in_i += nelem
            
            self.dtypes[i] = (fname, 'S%d' % (maxlen), nelem)
            logging.log(10, "assigned dtype: %s" % (self.dtypes[i],))
            
    def sealArray(self):
        """ Take the parsed list of records and generate a typed and named numpy array.

        Notes:
           I don't see any clever way of parsing an array of strings into an array. So
           put the records into an array of strings, then convert each column into the
           final array. Ugh.
        """

        if hasattr(self, 'array'):
            raise RuntimeError("%s already sealed" % (self.name))

        t0 = time.time()

        # Convert to an ndarray of strings, where the widths of
        # the columns are set by the widest element in that column (from .strlens)
        nrec = len(self.records)
        dtype = ','.join(["S"+str(x) for x in self.strlens])
        logging.log(10, "na.dtype: %s" % (dtype))
        ta = np.array(self.records, dtype=dtype)
        
        # Calculate the proper lengths for all string columns.
        self.fixupS0s(ta)
        logging.log(10, self.dtypes)
        t1 = time.time()

        # Create our output array, with the proper final numpy types. We then
        # convert each column from the string array.
        #
        # Any multi-element input arrays have been flattened together, and the 
        # corresponding output arrays need to be reconstructed.
        #
        a = np.zeros(nrec, dtype=self.dtypes)
        in_i = 0                        
        for i in range(len(self.dtypes)):
            fname, dtype, nelem = self.dtypes[i]
            if nelem > 1:
                # Transfer/convert a range of python array elements into the typed
                # elements of a numpy vector.
                for f in range(nelem):
                    fn = 'f%d' % (in_i+f)
                    a[fname][:,f] = np.array(ta[fn], dtype=dtype)
                in_i += nelem
            else:
                fn = 'f%d' % (in_i)
                a[fname] = np.array(ta[fn], dtype=dtype)
                in_i += 1

        self.array = a
        self.recarray = a.view(np.recarray)

        t2 = time.time()
        logging.log(10, "step1=%0.2f step2=%0.2f" % (t1-t0,t2-t1))

    def asArray(self):
        """ Return the structure as a numpy array. Generates and caches it if need be. """

        # Caching is evil, Loomis. Don't do that.
        if not hasattr(self, 'array'):
            self.sealArray()
        return self.recarray

    def asObjlist(self, arrayRows=None):
        """ Return the structure as a list of typed objects. Generates and caches it if need be. """

        if not arrayRows:
            arrayRows = self.asArray()
        
        # So what name do you give what you want to be an anonymous class?
        if not hasattr(self, 'objClass'):
            self.objClass = type('YPFStruct_%s_%d' % (self.name, id(self)),
                                 (object,),
                                 {'__slots__':tuple([d[0] for d in self.dtypes])})

        def makeFromRow(objclass, row):
            assert len(row) == len(objclass.__slots__)
            obj = objclass()
            for i in range(len(row)):
                setattr(obj, obj.__slots__[i], row[i].tolist())
            return obj

        return [makeFromRow(self.objClass, r) for r in arrayRows]
        
class YPF(object):
    """ YPFs can contain three kinds of thing:
          - structs,
             'typedef struct {
                 BIF;
                 BAF;
              } STRUCTNAME;
             '
          - enums,
             'typedef enum {
                 BIF,
                 BAF
              } ENUMNAME;
             '
          - named variables
             "name blah blah blah"
             
          yp = YPF(fromFile=filename [, template=True])
          yp.write

    Limitations:
      - when we write a file, it is always in a fixed order:
         variables
         enums
         structs
         named variables
    """

    lineContRE = re.compile('\\[ \t]*$')
    structOrEnumRE = re.compile('typedef\s+(struct|enum)\s+{')
    
    def __init__(self,
                 fromFile=None,
                 fromString=None,
                 template=False):
        """ Construct a YPF.

        Args:
           fromFile    - the name of a file to parse.
           fromString  - a string to parse
           template    - if True, then delete any data values, leaving only
                         the typedefs.

        Notes:
           If neither fromstring or fromFile is specified, returns a blank YPF.
        """

        self.EOL = '\n'
        
        self.vars = {}
        self.enums = {}
        self.structs = {}

        if fromFile and fromString:
            raise RuntimeError("cannot create a YPF from both fromFile and fromString")

        self.filename = None
        if fromFile:
            self.filename = fromFile
            self.parseFile(fromFile)
        if fromString:
            self.parseString(fromString)
            
        if template:
            for v in self.vars:
                v.clear()
            for e in self.enums:
                e.clear()
            for s in self.structs:
                s.clear()

    def getline(self, s):
        """ Generator for returning merged lines. Assume that s is small enough
        that we can simply reda the whole thing.
        """
        
        lines = s.split(self.EOL)           # figure out EOL first?
        lidx = 0
        l = ""
        while lidx < len(lines):
            # I don't allow for comments after line continuations
            l += lines[lidx].strip()
            lidx += 1
            
            if len(l) >= 1 and l[-1] == '\\':
                l = l[:-1].strip() + " "
                if lidx == len(lines):
                    sys.stderr.write('WARNING: last line ends with a backslash.\n')
            else:
                yield l
                l = ""
        if l:
            yield l
        return

    def parseTypedef(self, l, lines):
        if re.match('typedef\s+struct', l):
            nlines, name, match = YPFStruct.parseDef(l, lines)
            self.structs[name] = YPFStruct(name, fields=match, enums=self.enums)
        elif re.match('typedef\s+enum', l):
            nlines, name, match = YPFEnum.parseDef(l, lines)
            self.enums[name] = YPFEnum(name, match)
        return nlines
    
    def parseString(self, s):
        """ Parse a string into our declarations and definitions.
        """

        t0 = time.time()
        lines = self.getline(s)
        lineno = 0
        for l in lines:
            lineno += 1
            logging.log(10, "raw line %05d: %s" % (lineno, l))
            if len(l) == 0 or l[0] == '#':
                continue
            
            if l.startswith('typedef'):
                lidx = self.parseTypedef(l, lines)
                lineno += lidx
            else:
                # Not a typedef -- see if the 1st token matches a known
                # structure name. If not, create a new variable.
                sidx = l.find(' ')
                if sidx > 0:
                    name = l[0:sidx]
                    struct = self.structs.get(name.upper(), None)
                    if struct:
                        struct.parseOne(l)
                    else:
                        v = YPFVar(l, debug=0)
                        if v.name in self.vars:
                            newValue = v.value
                            oldValue = self.vars[v.name].value
                            if newValue != oldValue:
                                print("Variable %s is being defined with a new value, overwriting it. old=%s, new=%s" 
                                      % (v.name, oldValue, newValue))
                        self.vars[v.name] = v
    
    def parseFile(self, filename):
        """ Parse an entire parameter file.

        Simply reads the whole file and parses that string.
        """

        f = open(filename, "r")
        s = f.read()
        f.close()

        logging.log(10, 'parsing filename %s: %d lines' % (filename, len(s)))

        self.parseString(s)

    def asString(self):
        """ Return all we know as a single string.

        Returns our contents in a fixed order:
          1- vars
          2- enum definitions
          3- struct definitions
          4- table rows.

        All sections are returned sorted alphabetically.

        Returns:
          - a string
        """

        res = []
        for v in self.vars.values():
            res.append(v.asString())
        res.append('')
        for e in self.enums.values():
            res.append(e.asString())
            res.append('')
        for s in self.structs.values():
            res.append(s.defAsString())
            res.append('')
        for s in self.structs.values():
            res.append(s.dataAsString())

        return '\n'.join(res)

    def writeFile(self, filename):
        """ Write the data to the given file. """

        s = self.asString()
        if os.access(filename, os.F_OK):
            raise RuntimeError("file %s already exists -- not overwritten." % (filename))
        
        f = file(filename, "w")
        f.write(s)
        f.close()
              
    def __getitem__(self, name):
        """ x = ypf[NAME]. For variables and structures """
        
        # Can you have a variable and a structure with the same name?
        if name in self.vars:
            return self.vars[name]
        
        name = name.upper()
        if name in self.structs:
            return self.struct[name]

        raise KeyError('%s not found as a variable or structure' % (name))
            
    def __setitem__(self, name, val):
        """ ypf[NAME] = VAL. For variables only. """

        if name in self.vars:
            l[name].setVal(val)
        else:
            l[name] = YPFVal(name, val)

def readOneStruct(filename, structName):
    """ Convenience function: returns a single named structure from a YPF. You would then need to call .asArray() or asObjlist() """
    ypf = YPF(filename)
    return ypf.structs[structName.upper()]

