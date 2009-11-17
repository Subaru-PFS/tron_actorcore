import re
import opscore.protocols.types as types

def help(actorName, cmdName, vocab, keys, pageWidth=80, html=False):
    """Return a help string for command cmdName, formatted for a line length of pageWidth"""

    cmd = [p for p in vocab if p[0] == cmdName]

    if not cmd:
        return "Unknown command %s" % cmdName
    assert len(cmd) == 1
    cmd = cmd[0]

    args = parse(cmd[1], keys)
    docstring = cmd[2].__doc__

    helpStr = ""
    if html:
        helpStr += "<DT>%s %s" % (actorName, cmdName)
    else:
        helpStr += "Usage: %s %s" % (actorName, cmdName)
        
    for a in args:
        if not html and len(helpStr.split("\n")[-1]) + len(str(a)) + 1 > pageWidth:
            helpStr += "\n%s" % (len(cmd[0])*" ")

        helpStr += " %s" % (str(a))
    if html:
        helpStr += "</DT>\n<DD>"
    helpStr += "\n"

    helpStr += "\n%s" % (formatString(docstring.split("\n")[0], pageWidth))

    if args:
        if html:
            helpStr += "\n<TABLE>"
        else:
            helpStr += "\nArguments:"
            
        for a in sorted(args, lambda a, b: cmp(a.name, b.name)):
            for k in a.key:
                extra = a.extraDescrip()

                if html:
                    lineFmt = "\n<TR><TD>&nbsp;&nbsp;&nbsp;&nbsp;</TD><TD>%s</TD><TD>%s</TD><TD>%s</TD></TR>"

                    helpStr += lineFmt % (k.name, k.help, "")
                else:
                    helpStr += "\n\t%-35s %s" % (k.name, k.help)

            if extra:
                for e in extra:
                    if html:
                        helpStr += lineFmt % ("", "", e)
                    else:
                        helpStr += "\n\t%-35s            %s" % ("", e)
        if html:
            helpStr += "\n</TABLE>"

    moreHelp = "\n".join(docstring.split("\n")[1:])
    if moreHelp:
        helpStr += "\n\n"

        formatted = []
        for para in re.split("\n\s*\n", moreHelp):
            formatted.append(formatString(para, pageWidth))

        helpStr += ("\n\n" + ("<P>" if html else "")).join(formatted)

    return helpStr

class Cmd(object):
    def __init__(self, keys, name, optional, needsValue, alternates):
        self.name = name
        self.optional = optional
        self.needsValue = needsValue
        self.alternates = alternates
        self.key = [keys[n.lower()] for n in name.split("|")]
        self.__parseKeys()

    class Arg(object):
        def __init__(self, name, isArray=False):
            self.name = name
            self.isArray = isArray
            self.values = []

    def __parseKeys(self):
        """Parse the keys"""

        self.args = []

        for k in self.key:
            kTypes = k.typedValues.vtypes
            isArray = True if len(kTypes) > 1 else False
            arg = Cmd.Arg(k.name, isArray)
            self.args.append(arg)

            for kt in kTypes:
                if isinstance(kt, types.Bool):
                    arg.values.append([kt, "BOOL"])
                elif isinstance(kt, types.Enum):
                    helpVals = [e[1] for e in kt.descriptors[1:]]
                    arg.values.append([kt, "|".join([e.split()[0] for e in helpVals]), helpVals])
                elif isinstance(kt, types.Int):
                    arg.values.append([kt, "N"])
                elif isinstance(kt, types.Float):
                    arg.values.append([kt, "FF.F"])
                elif isinstance(kt, types.String):
                    arg.values.append([kt, "\"SSS\""])
                else:
                    print "RHL", kt
                    #import pdb; pdb.set_trace()
                    arg.values.append([kt, "???"])

    def getArgs(self):
        if len(self.args) == 0:
            return ""
        elif len(self.args) > 1:
            print "XXXXXXXXXXXXX self.args = ", " ".join([str(a) for a in self.args])

        a = self.args[0]
        argVal = ""
        for v in a.values:
            if argVal:
                argVal += ", "

            argVal += v[1]

        if a.isArray:
            argVal = "%s[, %s, ...]" % (argVal, argVal)

        return argVal

    def getArgs_SAVE(self, enumHelp=False):
        for k in self.key:
            kTypes = k.typedValues.vtypes
            isArray = True if len(kTypes) > 1 else False

            argVal = ""
            for kt in kTypes:
                if argVal:
                    argVal += ", "

                if isinstance(kt, types.Bool):
                    argVal += "BOOL"
                elif isinstance(kt, types.Enum):
                    helpVals = [e[1] for e in kt.descriptors[1:]]
                    if enumHelp:
                        argVal += "|".join(["\t".join(e.split()) for e in helpVals])
                    else:
                        argVal += "|".join([e.split()[0] for e in helpVals])
                elif isinstance(kt, types.Int):
                    argVal += "N"
                elif isinstance(kt, types.Float):
                    argVal += "FF.F"
                elif isinstance(kt, types.String):
                    argVal += "\"SSS\""
                else:
                    print "RHL", kt
                    #import pdb; pdb.set_trace()
                    argVal += "???"

            if isArray:
                argVal = "%s[, %s, ...]" % (argVal, argVal)

            return argVal

    def __str__(self):
        argStr = ""
        if self.optional:
            argStr += "["

        if not self.needsValue:
            argStr += self.name
        else:
            argStr += "%s=%s" % (self.name, self.getArgs())

        if self.optional:
            argStr += "]"

        return argStr

    def extraDescrip(self):
        """Return an extra piece of text, usually an enum's per-value help"""
        try:
            return self.args[0].values[0][2]
        except IndexError:
            return None

def parse(argString, keys):
    """Parse a command string, making use of information in the keys dictionary"""
    keys = keys.keys

    args = []
    for a in argString.split():
        optional = False
        mat = re.search(r"\[([^]]+)\]", a)
        if mat:
            optional = True
            a = mat.group(1)

        needsValue = False
        mat = re.search(r"<([^>]+)>", a)
        if mat:
            needsValue = True
            a = mat.group(1)

        alternates = False
        mat = re.search(r"\(([^>]+)\)", a)
        if mat:
            alternates = True
            a = mat.group(1)

        args.append(Cmd(keys, a, optional, needsValue, alternates))

    return args

def formatString(para, pageWidth):
    """Format a string to have line lengths < pageWidth"""
    para = " ".join(para.split("\n")).lstrip()

    outStr = []
    outLine = ""
    for word in para.split():
        if outLine and len(outLine) + len(word) + 1 > pageWidth:
            outStr.append(outLine)
            outLine = ""

        if outLine:
            outLine += " "
        outLine += word

    if outLine:
        outStr.append(outLine)

    return "\n".join(outStr)