#!/usr/bin/env python

import os
import base64
import numpy
import pyfits

import opscore.protocols.keys as keys
import opscore.protocols.types as types

from opscore.utility.qstr import qstr

class McsCmd(object):

    def __init__(self, actor):
        # This lets us access the rest of the actor.
        self.actor = actor

        # Declare the commands we implement. When the actor is started
        # these are registered with the parser, which will call the
        # associated methods when matched. The callbacks will be
        # passed a single argument, the parsed and typed command.
        #
        self.vocab = [
            ('ping', '', self.ping),
            ('status', '', self.status),
            ('filter', '<filterName>', self.setFilter),
            ('expose', '@(bias|test)', self.expose),
            ('expose', '@(dark|object) <expTime>', self.expose),
            ('centroid', '<expTime>', self.centroid),
            ('reconnect', '', self.reconnect),
        ]

        # Define typed command arguments for the above commands.
        self.keys = keys.KeysDictionary("mcs_mcs", (1, 1),
                                        keys.Key("expTime", types.Float(), help="The exposure time"),
                                        keys.Key("filterName", 
                                                 types.Enum('J','H','K', 
                                                            help="The desired filter wheel")),
                                        )


    def ping(self, cmd):
        """Query the actor for liveness/happiness."""

        cmd.warn("text='I am running a fake camera'")
        cmd.finish("text='Present and (probably) well'")

    def reconnect(self, cmd):
        """ Make a completely new connection to our camera. Any existing connection is closed. """

        self.actor.connectCamera(cmd, doFinish=False)
        cmd.finish('text="camera (re-)connected!"')
        
    def setFilter(self, cmd):
        """ Set the filter wheel. """

        filterName = cmd.cmd.keywords["filterName"].values[0]
        self.actor.filterDevice.setFilter(cmd, filterName)

    def status(self, cmd):
        """Report camera status and actor version. """

        self.actor.sendVersionKey(cmd)
        self.actor.camera.sendStatusKeys(cmd)
        
        cmd.inform('text="Present!"')
        cmd.finish()

    def getNextFilename(self, cmd):
        """ Fetch next image filename. 

        In real life, we will instantiate a Subaru-compliant image pathname generating object.  

        """
        
        self.actor.exposureID += 1
        path = os.path.join("$ICS_MHS_DATA_ROOT", 'mcs')
        path = os.path.expandvars(os.path.expanduser(path))

        if not os.path.isdir(path):
            os.makedirs(path, 0o755)
            
        return os.path.join(path, 'MCSA%010d.fits' % (self.actor.exposureID))

    def _doExpose(self, cmd, expTime, expType):
        """ Take an exposure and save it to disk. """
        
        image = self.actor.camera.expose(cmd, expTime, expType)
        filename = self.getNextFilename(cmd)
        pyfits.writeto(filename, image, checksum=False, clobber=True)
        cmd.inform("filename=%s" % (qstr(filename)))
        
        return filename, image
            
    def expose(self, cmd):
        """ Take an exposure. Does not centroid. """

        expType = cmd.cmd.keywords[0].name
        if expType in ('bias', 'test'):
            expTime = 0.0
        else:
            expTime = cmd.cmd.keywords["expTime"].values[0]

        filename, image = self._doExpose(cmd, expTime, expType)
        cmd.finish('exposureState=done')

    def centroid(self, cmd):
        """ Take an exposure and measure centroids. """

        expTime = cmd.cmd.keywords["expTime"].values[0]
        expType = 'object' if expTime > 0 else 'test'

        filename, image = self._doExpose(cmd, expTime, expType)

        # The encoding scheme is temporary, and will become encapsulated.
        cmd.inform('state="measuring"')
        centroids = numpy.random.random(4800).astype('f4').reshape(2400,2)

        centroidsStr = self._encodeArray(centroids)
        cmd.inform('state="measured"; centroidsChunk=%s' % (centroidsStr))

        cmd.finish('exposureState=done')

    def _encodeArray(self, array):
        """ Temporarily wrap a binary array transfer encoding. This will be provided by the opscore library. """

        # Actually, we want dtype,naxis,axNlen,base64(array)
        return base64.b64encode(array.tostring())
        
