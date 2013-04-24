import numpy
import time

class Camera(object):
    pass

class FakeCamera(Camera):
    def __init__(self):
        # Should read from config file....
        self.imageSize = (4096, 4096)
        self.biasLevel = 100
        self.readNoise = 5.0
        self.name = 'numpy_fake'
        
    def _readoutTime(self):
        return 0.5

    def _exposureOverheadTime(self):
        return 0.1

    def getCcdTemp(self):
        return -55.5

    def sendStatusKeys(self, cmd):
        """ Send all our status keys to the given command. """ 

        cmd.inform('cameraName=%s; readNoise=%0.2f; ccdTemp=%0.1f' % (self.name, 
                                                                      self.readNoise,
                                                                      self.getCcdTemp()))
        
    def expose(self, cmd, expTime, expType):
        """ Generate an 'exposure' image. We don't have an actual camera, so generate some 
        plausible image. 

        Args:
           cmd     - a Command object to report to. Ignored if None.
           expTime - the fake exposure time. 
           expType - ("bias", "dark", "object", "flat", "test")
           
        Returns:
           - the image.

        Keys:
           exposureState
        """

        if not expType:
            expType = 'test'
        if cmd:
            cmd.inform('exposureState="exposing"')
        if expType not in ('bias', 'test') and expTime > 0:
            time.sleep(expTime + self._exposureOverheadTime())

        if cmd:
            cmd.inform('exposureState="reading"')
        image = numpy.ones(shape=self.imageSize).astype('u2')
        #image = numpy.random.normal(self.biasLevel, 
        #                            scale=self.readNoise, 
        #                            size=self.imageSize).astype('u2')

        if expType != 'test':
            time.sleep(self._readoutTime())
        return image
        
        
