from __future__ import print_function
__all__ = ['qstr']

def qstr(o, tquote='"', equotes=None, doNewlines=True):
    """ Put a string representation of an object into quotes and escape it minimally.
    
    Return the string wrapped in tquotes.
    Escape all the characters in equotes, as well as backslashes. If equotes are
    are not defined, use tquote.
    
    Basically, 
       o     -> "str(o)"
       \     -> \\
       quot  -> \quot
       NL    -> \n
       CR    -> \r

    repr(o) does too much, and prefers single quotes to boot.    

    Perhaps NULs should be mangled. As long as I'm in an 8-bit clean world I won't
    """
  
    s = str(o)
    
    # Always quote backslashes _first_.
    #
    if equotes == None:
        if tquote == None:
            return s
        equotes = '\\' + tquote
    else:
        equotes = '\\' + tquote + equotes    
     
    # Could compare with a clever RE scheme:
    #   matches = match_all(equotes)
    #   '\\'.join(match pieces)
    #
    for equote in equotes:
        equote_repl = "\\" + equote
        s = s.replace(equote, equote_repl)
    
    if doNewlines:
        s = s.replace('\n', '\\n')
        s = s.replace('\r', '\\r')
        
    if tquote:
        return tquote+s+tquote
    else:
        return s
            

if __name__ == "__main__":
    tests = ('', 
             'a', 
             '"',
             "'",
             '""',
             "''",
             "\'",
             '\"',
             '\\',
             'abcdef',
             'a"b\"c\'d\\e\\',
             chr(7), chr(12), chr(255), chr(10), chr(13), chr(0))
    
    for t in tests:
        qt = qstr(t)
        try:
            e = eval(qt)
        except Exception as e:
            print("===== NE: %r" % (t))
            print("        : %r" % (qt))
            print("                 error: %s" % (e))
            continue
        if t == e:
            print("===== OK: %s" % (qt))
        else:
            print("===== NG: %r:" % (t))
            print("        : %r:" % (qt))
    print()
    print()

    for t in tests:
        qt = qstr(t, tquote="'")
        try:
            e = eval(qt)
        except Exception as e:
            print("===== NE: %r" % (t))
            print("        : %r" % (qt))
            print("                 error: %s" % (e))
            continue
        if t == e:
            print("===== OK: %s" % (qt))
        else:
            print("===== NG: %s:" % (t))
            print("        : %s:" % (qt))

    print()
    print()
