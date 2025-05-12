import logging
import socket
import sys

from rfc5424logging import Rfc5424SysLogHandler

""" Configure python loggers to use an rsyslog handler.

We set up the system's root Logger, from which all others
inherit. That logger has given two Handlers: one which emits to a
localhost rsyslog server which is configured to use RFC5424 messages,
and one which writes to stderr.

The syslog handler is set to forward all messages which make it
through the loggers, by setting level=DEBUG.

The stderr handler (named consoleHandler) is configured based on the
nature of stderr. If that is a terminal emit all logger messages (so
handler.level=DEBUG). If that is a file, only emit CRITICAL ones.

This module's `setupRootLogger` does all of that, and must be called
(very) early on. The reason something needs to be called at all is
that we need to label all messages with the actor's name, which is not
known to the base python logging module.

"""

# Configure the default formatter and logger. This does some magic internal logging module configuration.
logging.basicConfig(datefmt = "%Y-%m-%d %H:%M:%S",
                    format = "%(asctime)s.%(msecs)03dZ %(name)-16s %(levelno)s %(filename)s:%(lineno)d %(message)s")

class OpsLogFormatter(logging.Formatter):
    def __init__(self):
        """Defines the canonical PFS log format. From now, always local time. """

        dateFmt = "%Y-%m-%d %H:%M:%S"
        fmt = "%(asctime)s.%(msecs)03d %(name)-16s %(levelno)s %(filename)s:%(lineno)d %(message)s"

        logging.Formatter.__init__(self, fmt, dateFmt)

try:
    rootHandler
except:
    rootHandler = None
    consoleHandler = None

def setConsoleLevel(level):
    """Set the logging level for the stderr *Handler*. """
    if not consoleHandler:
        logging.critical('the root logger must be setup via pfslogging.setupRootHandler() before the console level can be set.')
        return
    logging.debug(f'setting console handler logging level to {level}')
    consoleHandler.setLevel(level)

def makeOpsHandler(basename : 'str', serverAddr=('127.0.0.1', 11514)) -> logging.Handler:
    """Create the syslog Handler. We use TCP, for rsyslog reasons.

    I do not like using TCP here -- that substantial buffering can
    hide horrors. We are on localhost so could use anything. But did
    not want to be responsible for configuration of a local socket.

    """
    handler = Rfc5424SysLogHandler(address=serverAddr,
                                   appname=basename,
                                   msg_as_utf8=False,
                                   socktype=socket.SOCK_STREAM)
    handler.setFormatter(OpsLogFormatter())
    return handler

def setupRootLogger(basename, level=logging.INFO, serverAddr=('127.0.0.1', 11514)):
    """ (re-)configure the root handler and logger to save all output to rsyslog, plus a console Handler . """

    global rootHandler
    global consoleHandler

    # Make sure we are able to delete/replace any existing handler
    lastRootHandler = rootHandler

    rootHandler = makeOpsHandler(basename, serverAddr=serverAddr)
    rootHandler.setLevel(logging.DEBUG)

    rootLogger = logging.getLogger()
    rootLogger.setLevel(level)
    if lastRootHandler:
        rootLogger.removeHandler(lastRootHandler)
    rootLogger.addHandler(rootHandler)

    # Shut stderr output down if we are not a terminal.
    for h in rootLogger.handlers:
        logging.debug(f'root handler: {h} {h.__class__}')
        if isinstance(h, logging.StreamHandler) and h.stream == sys.stderr:
            consoleHandler = h
            if h.stream.isatty():
                rootLogger.info('copying all logging to stderr')
                setConsoleLevel(logging.DEBUG)
            else:
                # Basically disable stderr output
                rootLogger.debug('disabling all but critical stderr output')
                setConsoleLevel(logging.CRITICAL)
    logging.debug('setup root logger under %s', basename)

    return rootLogger
