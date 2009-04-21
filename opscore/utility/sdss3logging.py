#!/usr/bin/env python

""" sdss3logging.py -- provide APO-standard shims for the python logging module.

APO's operational cycle is the MJD, rolled over at 10AM local time.
This sets the logging module's default Logger class. For nearly all uses, this
module only needs to be imported once, to do just that. After which once could get a logger with:

  import logging
  mylogger = logging.getLogger('name')

Todo:
   - Figure out how to _use_ these: whether to use the logging config file,
     whether to set up a root logger, whether to entirely wrap the logging.py
     functions, etc.
"""

import logging
import os
import os.path
import time
import types

class OpsLogFormatter(logging.Formatter):
    def __init__(self):
        """ Defines the canonical log format. Actually, only the timestamp format and 'zone' are canonical.

        Notes:
           We force "GMT"/UTC/Zulu time, but cannot tell if we are using UTC or TAI.
        """
        
        dateFmt = "%Y-%m-%d %H:%M:%S"
        fmt = "%(asctime)s.%(msecs)03dZ %(name)s %(levelno)s %(filename)s:%(lineno)d %(message)s"
        
        logging.Formatter.__init__(self, fmt, dateFmt)
        self.converter = time.gmtime
        
class OpsRotatingFileHandler(logging.StreamHandler):
    APOrolloverHour = 10.0              # Check this....
    
    def __init__(self, dirname='.', basename='', rolloverTime=None):
        """ create a logging.FileHandler which:
              - names logfiles by their opening date+time, to the second.
              - names the first file by the invocation date.
              - rolls subsequent files over at the APO MJD rollover.

         Args:
           dirname         ? which directory to create the files in. ['.']
           basename        ? a fixed prefix for the filenames, if any. ['']
           rolloverTime    ? override the default rollover time, in local hours [10:00 local]
         """

        logging.StreamHandler.__init__(self)
        
        self.dirname = dirname
        self.basename = basename
        self.formatter = OpsLogFormatter()
        
        if rolloverTime == None:
            self.rolloverTime = self.APOrolloverHour * 3600.0
        else:
            self.rolloverTime = rolloverTime
        if self.rolloverTime < 0 or self.rolloverTime >= 3600.0 * 24:
            raise RuntimeError("invalid rollover time specified: %s" % (self.rolloverTime))

        # Force file creation now.
        self.doRollover()
        
    def _setTimes(self, first=False):
        """ set .rolloverAt to the next one from now.
        
        Bug: should all be done in UTC, including .rolloverTime.
        """

        now = time.time()
        self.startTime = now

        # Get local midnight for the day.
        t = list(time.localtime(now))
        t[3] = t[4] = t[5] = 0
        
        self.rolloverAt = time.mktime(t) + self.rolloverTime
        
        # Add a day if we are past today's rolloverTime.
        if now >= self.rolloverAt:
            t[2] += 1
            self.rolloverAt = time.mktime(t) + self.rolloverTime

        assert(now < self.rolloverAt)

    def emit(self, record):
        """
        Emit a record.
        
        If a formatter is specified, it is used to format the record.
        The record is then written to the stream with a trailing newline
        [N.B. this may be removed depending on feedback]. If exception
        information is present, it is formatted using
        traceback.print_exception and appended to the stream.
        """
        if self.shouldRollover(record):
            self.doRollover()

        try:
            msg = self.format(record)
            fs = "%s\n"

            # This was copied from the logging module. Haven't thought about unicode.
            if not hasattr(types, "UnicodeType"): #if no unicode support...
                self.stream.write(fs % msg)
            else:
                try:
                    self.stream.write(fs % msg)
                except UnicodeError:
                    self.stream.write(fs % msg.encode("UTF-8"))
            self.flush()
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def shouldRollover(self, record):
        """
        Determine if rollover should occur
        
        record is not used, as we are just comparing times, but it is needed so
        the method siguratures are the same
        """
        print "now: %s, rollover+ %s" % (time.time(), self.rolloverAt)
        return time.time() >= self.rolloverAt

    def doRollover(self):
        """ We keep log files forever, and want them named by the start date.
        We also want a convenience current.log symbolic link to the new file.
        """

        self._setTimes()

        # get the time that this sequence starts at and make it a TimeTuple
        timeString = time.strftime("%Y-%m-%dT%H:%M:%S",
                                   time.gmtime(self.startTime))
        filename = self.basename + timeString + ".log"
        path = os.path.join(self.dirname, filename)
        
        if os.path.exists(path):
            # Append? Raise?
            raise RuntimeError("logfile %s already exists. Would append to it." % (path))

        if self.stream:
            self.stream.flush()
        self.stream = open(path, 'a+')
            
        # Fiddle the current.log link
        linkname = os.path.join(self.dirname, 'current.log')
        try:
            os.remove(linkname)
        except:
            pass
        try:
            os.symlink(filename, linkname)
        except Exception, e:
            print "Failed to create current.log symlink to %s" % (filename)
           
class OpsFileLogger(logging.Logger):
    def __init__(self, name, dirname='.', level=logging.INFO):
        logging.Logger.__init__(self, name, level=level)
        h = OpsRotatingFileHandler(dirname)
        self.formatter = OpsLogFormatter()
        self.addHandler(h)

# logging depends on module variables, unfortunately.
#
logging.setLoggerClass(OpsFileLogger)

def main():
    myLogger = logging.getLogger('level.fortytwo')
    #cheat here by forcing a rollover.
    myLogger.handlers[0].rolloverAt = time.time() + 10.0

    for s in range(20):
        myLogger.critical('crash rollover: %s', 'blam')
        time.sleep(1)

if __name__ == '__main__':
    main()
    
