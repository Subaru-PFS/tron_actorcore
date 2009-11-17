#
# A simple python interface to svn, using os.popen
#
# If ever we want to do anything clever, we should use one of
# the supported svn/python packages
#
import os, re, sys

def isSvnFile(file):
   """Is file under svn control?"""

   return re.search(r"is not a working copy",
                    "".join(os.popen("svn info %s 2>&1" % file).readlines())) == None

def getInfo(file="."):
   """Return a dictionary of all the information returned by "svn info" for the specified file"""

   if not isSvnFile(file):
       raise RuntimeError, "%s is not under svn control" % file

   infoList = os.popen("svn info %s" % file).readlines()

   info = {}
   for line in infoList:
       mat = re.search(r"^([^:]+)\s*:\s*(.*)", line)
       if mat:
           info[mat.group(1)] = mat.group(2)

   return info

def isTrunk(file="."):
   """Is file on the trunk?"""

   info = getInfo(file)

   return re.search(r"/trunk($|/)", info["URL"]) != None

def revision(file=None, lastChanged=False):
   """Return file's Revision as a string; if file is None return
   a tuple (oldestRevision, youngestRevision, flags) as reported
   by svnversion; e.g. (4123, 4168, ("M", "S")) (oldestRevision
   and youngestRevision may be equal)
   """

   if file:
       info = getInfo(file)

       if lastChanged:
           return info["Last Changed Rev"]
       else:
           return info["Revision"]

   if lastChanged:
       raise RuntimeError, "lastChanged makes no sense if file is None"

   res = os.popen("svnversion . 2>&1").readline()

   if res == "exported\n":
       raise RuntimeError, "No svn revision information is available"

   versionRe = r"^(?P<oldest>\d+)(:(?P<youngest>\d+))?(?P<flags>[MS]*)"
   mat = re.search(versionRe, res)
   if mat:
       matches = mat.groupdict()
       if not matches["youngest"]:
           matches["youngest"] = matches["oldest"]
           # OK, we have only one revision present.  Find the newest revision
           # that actually changed anything in this product and ignore "oldest" (#522)
           res = os.popen("svnversion --committed . 2>&1").readline()
           mat = re.search(versionRe, res)
           if mat:
               matches = mat.groupdict()
               return matches["youngest"], matches["youngest"], tuple(matches["flags"])

       return matches["oldest"], matches["youngest"], tuple(matches["flags"])

   raise RuntimeError, ("svnversion returned unexpected result \"%s\"" % res[:-1])

#
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
#
def guessVersionName(HeadURL, exitOnError=True):
   """Guess a version name given a HeadURL, and annotate that based on available revision info. 

   Returns a string suitable for a version identifier and possibly a tag.

   Args:
      exitOnError:    if True, blow up if the version is not sensible (has
                      modified files, switched revisions, etc.) 
"""

   isTag = False

   if re.search(r"/trunk$", HeadURL):
       versionName = "trunk"
   elif re.search(r"/branches/(.+)$", HeadURL):
       versionName = "branch_%s" % re.search(r"/branches/([^/]+).*$", HeadURL).group(1)
   elif re.search(r"/tags/(\d+(\.\d+)*)([-+][_a-zA-Z0-9]+)?$", HeadURL):
       versionName = "%s" % re.search(r"/tags/(.*)$", HeadURL).group(1)
       isTag = True
   elif re.search(r"/tickets/(\d+)$", HeadURL):
       versionName = "ticket_%s" % re.search(r"/tickets/(\d+).*$", HeadURL).group(1)
   else:
       print >> sys.stderr, "Unable to guess versionName name from %s" % HeadURL
       versionName = "unknown"

   try:                    # Try to lookup the svn versionName
       (oldest, youngest, flags) = revision()

       okVersion = True
       if oldest != youngest:
           msg = "You have a range of revisions in your tree (%s:%s); adopting %s" % \
                 (oldest, youngest, youngest)
           okVersion = False

       if "M" in flags:
           msg = "You have unchecked in files"
           okVersion = False
       if "S" in flags:
           msg = "You have switched SVN URLs"
           okVersion = False
       if "P" in flags:
           msg = "You have a partial checkout."
           okVersion = False

       if not okVersion:
          if exitOnError:
             raise RuntimeError, ("Problem with determining svn revision: %s" % msg)
          else:
             print >> sys.stderr, "Problem with determining svn revision: %s" % msg

       # In the nasty case where a tagged version has modified files, we want to 
       # rub noses in the fact.
       if not isTag or flags:
          versionName += "+svn" + youngest + ''.join(flags)

   except IOError:
      pass                      # No revision data available. 

   return versionName

def parseVersionName(versionName):
   """A callback that knows about the LSST convention that a tagname such as
   ticket_374
   means the top of ticket 374, and
   ticket_374+svn6021
   means revision 6021 on ticket 374.  You may replace "ticket" with "branch" if you wish

   The "versionName" may actually be the directory part of a URL, and ".../(branches|tags|tickets)/tagname"
   is also supported
   """

   mat = re.search(r"/(branche|tag|ticket)s/(\d+(?:\.\d+)*)(?:([-+])((svn)?(\d+)))?$", versionName)
   if not mat:
       mat = re.search(r"/(branch|ticket)_(\d+)(?:([-+])svn(\d+))?$", versionName)
   if mat:
       type = mat.group(1)
       if type == "branches":
           type = "branch"
       ticket = mat.group(2)
       pm = mat.group(3)               # + or -
       revision = mat.group(4)
       if revision:
           revision = re.sub("^svn", "", revision)

       return (type, ticket, revision, pm)

   return (None, None, None, None)

def simpleVersionName(HeadURL=None):
   """ Return a single string describing our svn version. 

   Use 'svn info' and 'svnversion' if we have a live version. Otherwise try to use HeadURL if that is available.
   """

   try:
      url = getInfo()['URL']
   except:
      url = HeadURL
   if not url:
      return "unknown"

   try:
      return guessVersionName(url, exitOnError=False)
   except:
      return "unknown"


if __name__ == "__main__":
   print simpleVersionName()
