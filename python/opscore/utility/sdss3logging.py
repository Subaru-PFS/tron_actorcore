#!/usr/bin/env python

""" sdss3logging.py -- provide APO-standard shims for the python logging module.

APO's operational cycle is the MJD, rolled over at 10AM local time.

For nearly all uses, this module only needs to be imported once, to
setup an application's named loggers. That import should happen
before any other 'import logging's, as it defines the default logging formatter.

  import sdss3logging
  import logging

  opsLogging.setRootLogger('/tmp/cpl/log1')
  
After which any other module can :

  import logging
  mylogger = logging.getLogger('name')

  mylogger.debug('something smells %s', 'here')
  myLogger.critical('fire in the %s', 'hold')
  
Todo:
   - Figure out how to _use_ these: whether to use the logging config file,
     whether to set up a root logger, whether to entirely wrap the logging.py
     functions, etc.
"""

import logging
import os
import os.path
import sys
import time
import types

# Configure the default formatter and logger.
logging.basicConfig(datefmt = "%Y-%m-%d %H:%M:%S",
                    format = "%(asctime)s.%(msecs)03dZ %(name)-16s %(levelno)s %(filename)s:%(lineno)d %(message)s")

class OpsLogFormatter(logging.Formatter):
    def __init__(self):
        """ Defines the canonical log format. Actually, only the timestamp format and 'zone' are canonical.

        Notes:
           We force "GMT"/UTC/Zulu time, but cannot tell if we are using UTC or TAI.
        """
        
        dateFmt = "%Y-%m-%d %H:%M:%S"
        fmt = "%(asctime)s.%(msecs)03dZ %(name)-16s %(levelno)s %(filename)s:%(lineno)d %(message)s"
        
        logging.Formatter.__init__(self, fmt, dateFmt)
        self.converter = time.gmtime
        
class OpsRotatingFileHandler(logging.StreamHandler):
    APOrolloverHour = 24 * 0.3
    
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
        self.stream = None              # StreamHandler opens stderr, which we do not want to close.
        
        self.dirname = os.path.expandvars(os.path.expanduser(dirname))
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
        
    def _setTimes(self, startTime=None):
        """ set .rolloverAt to the next one from now.
        
        Bug: should all be done in UTC, including .rolloverTime.
        """

        if startTime:
            now = startTime
        else:
            now = time.time()

        self.startTime = now

        # Get local midnight for the day.
        t = list(time.localtime(now))
        t[3] = t[4] = t[5] = 0
        self.rolloverAt = time.mktime(t) + self.rolloverTime
        
        # Add a day if we are past today's rolloverTime.
        if now >= self.rolloverAt:
            self.rolloverAt += 24*3600
        
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
            self.doRollover(record=record)

        #logging.StreamHandler.emit(self, record)
        #return
    
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
        """ Determine if rollover should occur, based on the timestamp of the LogRecord. """

        dt = self.rolloverAt - record.created
        if dt < 0:
            return True

        if dt > 24*3600:
            sys.stderr.write('shouldRollover %s >= %s = %s\n' %
                             (record.created, self.rolloverAt, record.created >= self.rolloverAt))

        return False

    def doRollover(self, record=None):
        """ We keep log files forever, and want them named by the start date.
        We also want a convenience current.log symbolic link to the new file.
        """

        startTime=(record.created if record else None)
        self._setTimes(startTime=startTime)

        # get the time that this sequence starts at and make it a TimeTuple
        timeString = time.strftime("%Y-%m-%dT%H:%M:%S",
                                   time.gmtime(self.startTime))
        filename = self.basename + timeString + ".log"

        try:
            os.makedirs(self.dirname, 0755)
        except OSError, e:
            pass
            
        path = os.path.join(self.dirname, filename)
        
        if os.path.exists(path):
            # Append? Raise?
            raise RuntimeError("logfile %s already exists. Would append to it." % (path))

        oldStream = self.stream
        try:
            self.stream = open(path, 'a+')
        except Exception, e:
            sys.stderr.write("Failed to rollover to new logfile %s: %s\n" % (path, e))
            return
            
        self.filename = path

        if oldStream:
            oldStream.flush()
            oldStream.close()

        # Fiddle the current.log link
        linkname = os.path.join(self.dirname, '%scurrent.log' % (self.basename))
        try:
            os.remove(linkname)
        except:
            pass
        try:
            os.symlink(filename, linkname)
        except Exception, e:
            print "Failed to create current.log symlink to %s" % (filename)

           
def makeOpsFileHandler(dirname, basename='', propagate=True):
    """ create a rotating file handler with APO-style filenames and timestamps..

    Args:
        dirname    - directory name for the logs. Must already exist.
        name       - name of the logging system.
        basename   ? If set, a prefix to the filenames. ['']
    """
    
    handler = OpsRotatingFileHandler(dirname=dirname, basename=basename)
    handler.setFormatter(OpsLogFormatter())

    return handler

def makeOpsFileLogger(dirname, name, basename='', propagate=True):
    """ create a rotating file logger with APO-style filenames and timestamps..

    Args:
        dirname    - directory name for the logs. Must already exist.
        name       - name of the logging system.
        basename   ? If set, a prefix to the filenames.
        propagate  ? If set, propagate log messages higher up the name tree. [True] 
    """
    
    tlog = logging.getLogger(name)
    tlog.propagate = propagate

    handler = makeOpsFileHandler(dirname, basename=basename)
    tlog.addHandler(handler)

    return tlog

try:
    rootHandler
except:
    rootHandler = None
    consoleHandler = None

def setConsoleLevel(level):
    if not consoleHandler:
        logging.critical('the root logger must be setup via sdss3logging.setupRootHandler() before the console level can be set.')
        return
        
    consoleHandler.setLevel(level)
        
def setupRootLogger(basedir, level=logging.INFO):
    """ (re-)configure the root logger to save all output to a APO-style rotating file Handler, plus a console Handler. """

    global rootHandler
    global consoleHandler

    # Make sure we are able to delete/replace any existing handler
    lastRootHandler = rootHandler

    rootHandler = makeOpsFileHandler(basedir)
    rootHandler.setLevel(logging.DEBUG)

    rootLogger = logging.getLogger()
    rootLogger.setLevel(level)
    if lastRootHandler:
        rootLogger.removeHandler(lastRootHandler)
    rootLogger.addHandler(rootHandler)

    # Shut stdout output down if we are not a terminal.
    for h in rootLogger.handlers:
        if isinstance(h, logging.StreamHandler) and h.stream == sys.stderr:
            consoleHandler = h
            if h.stream.isatty():
                setConsoleLevel(level)
            else:
                # Basically disable stderr output
                rootLogger.warn('disabling all but critical stderr output')
                setConsoleLevel(logging.CRITICAL + 1)
        
    return rootLogger
    
def main():
    consoleLogger = logging.getLogger()

    consoleLogger.setLevel(logging.INFO)
    
    myLogger = makeOpsFileLogger('/tmp', 'tlog')
    myLogger.setLevel(logging.DEBUG)
    # Force a rollover by cheating.
    myLogger.handlers[0].rolloverAt = int(time.time() + 10.0)

    # Should propagate up to the root as well as log to its own logfile.
    c2Logger = logging.getLogger('c2')
    c2Logger.setLevel(logging.DEBUG)
    h2 = makeOpsFileHandler('/tmp', basename='c2-')
    h2.setLevel(logging.WARN)
    c2Logger.addHandler(h2)
    
    consoleLogger.info('max s = %d', 20)
    c2Logger.critical('me too! max s = %d', 20)
    for s in range(150000):
        myLogger.critical('crash rollover: %05d', s)
        if s % 10000 == 0:
            c2Logger.info('s=%d, logname=%s', s, myLogger.handlers[0].filename)

if __name__ == '__main__':
    main()
    
