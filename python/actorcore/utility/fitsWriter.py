import logging
from logging import log
from multiprocessing import Process, Queue
import numpy as np
import os
import pathlib
import tempfile
import time
import threading

import fitsio

class FitsWriter(object):
    def __init__(self, inQ, outQ):
        """Buffer FITS file writes.

        Args:
        inQ : `Queue`
            Queue we receive commands on
        outQ : `Queue`
            Queue we report success/failure to.

        Queue commands are:
        'create', path, header : create new FITS file, with given PHDU
        'write', hduId, extname, data, hdr : append new HDU to our file.
        'close' : finish out the current file. Rename to final name if we are writing to a scratch file.
        'shutdown': exits process after closing any open file.

        where:

        path : FITS pathname we eventually generate
        hduId : identifier we return when this HDU is written.
        extname : the FITS EXTNAME we identify this HDU with
        data : None or an image. We compress with RICE_1, so should be uint16
        header : a fitsio-compliant list of dicts
        """
        self.logger = logging.getLogger('fitsWriter')
        self.inQ = inQ
        self.outQ = outQ

        self.currentPath = None
        self.tempPath = None
        self.currentFits = None

    @staticmethod
    def setupFitsWriter(inQ, outQ):
        """Convenience wrapper for Process creation."""
        fw = FitsWriter(inQ, outQ)
        fw.run()

    def report(self, command, *, path=None, hduId=None, status=None, errorDetails=None):
        try:
            self.outQ.put(dict(command=command, status=status,
                               path=path, hduId=hduId,
                               errorDetails=errorDetails))
        except Exception as e:
            self.logger.warning(f'failed to send a command to self.outQ: {e}')

    def reportSuccess(self, command, *, path=None, hduId=None):
        self.report(command, path=path, hduId=hduId, status='OK', errorDetails=None)

    def reportFailure(self, command, *, path=None, hduId=None, errorDetails='ERROR'):
        self.report(command, path=path, hduId=hduId, status='ERROR', errorDetails=errorDetails)

    def create(self, path, header, useTemp=True):
        """Create a new FITS file and generate the PHDU

        Parameters
        ----------
        path : path-like
            The full pathname of the FITS file
        header : fitsio card dict
            The PHDU cards
        useTemp : bool
            Whether to write to a temp file, then rename at end.
        """
        try:
            self.currentPath = path
            if useTemp:
                path = pathlib.Path(path)
                dirname = path.parent

                # (Ab)use tempfile machinery. We actually need to have fitsio do the
                # file creation, so pretend we have the deprecated mktemp() to get a
                # filename, by deleting the tempfile we cannot use.
                tempFile, self.tempPath = tempfile.mkstemp(suffix='.fits', dir=dirname)
                os.close(tempFile)
                os.remove(self.tempPath)
                self.currentFits = fitsio.FITS(self.tempPath, mode='rw')
            else:
                self.tempPath = None
                self.currentFits = fitsio.FITS(self.currentPath, mode='rw')

        except Exception as e:
            self.logger.warning(f'failed to create {path}: {e}')
            self.reportFailure('CREATE', path=path,
                               errorDetails=f'ERROR creating {path}(tempPath={self.tempPath}): {e}')
            self.currentPath = None
            self.currentFits = None
            self.tempPath = None

        try:
            self.currentFits.write(None, header=header)
            self.reportSuccess('CREATE', path=self.currentPath)
        except Exception as e:
            self.logger.warning("failed to write PHDU to {self.currentFits}")
            self.reportFailure('CREATE', path=self.currentPath,
                               errorDetails='ERROR writing PHDU: {e}')

    def write(self, hduId, extname, data, header):
        """Append an image HDU to our file

        Parameters
        ----------
        hduId : object
            Whatever the main program sent to identify this HDU
        extname : `str``
            The FITS EXTNAME for this HDU
        data : None or `np.array`
            The image data. We always RICE_1 compress, so this should be uint16.
        header : fitsio-compliant cardlist
            The header cards for this HDU
        """

        try:
            compress = None # if data is None else 'RICE_1'
            self.currentFits.write(data, header=header, extname=extname, compress=compress)

            if data is not None:
                self.currentFits[-1].write_checksum()
            self.reportSuccess('WRITE', path=self.currentPath, hduId=hduId)
        except Exception as e:
            self.logger.warning(f'failed to write hdu {hduId} for {self.currentPath}: {e}')
            self.reportFailure('WRITE', path=self.currentPath, hduId=hduId,
                               errorDetails=f'ERROR writing {hduId}: {e}')

    def close(self):
        """Actually close out any open FITS file.

        If we are writing to a tempfile, rename it to the real name.
        """
        self.logger.info(f'closing {self.currentPath}')
        try:
            try:
                self.currentFits.close()
            except Exception as e:
                self.logger.warning(f'failed to close {self.currentFits}: {e}')
                # Keep going....

            if self.tempPath is not None:
                # Rename temp file to final name
                self.logger.info(f'renaming {self.tempPath} to {self.currentPath}')
                os.rename(self.tempPath, self.currentPath)
            self.reportSuccess('CLOSE', path=self.currentPath)
        except Exception as e:
            self.logger.warning(f'failed to close out {self.currentPath}: {e}')
            self.reportFailure('CLOSE', path=self.currentPath,
                               errorDetails=f'ERROR closing or renaming {self.currentPath}(temp={self.tempPath}): {e}')
        finally:
            self.currentFits = None
            self.currentPath = None
            self.tempPath = None

    def run(self):
        """Main process loop. Read and execute commands from .inQ """

        self.logger.info('starting loop...')
        while True:
            cmd = None
            try:
                self.logger.info('waiting for new command...')
                t0 = time.time()
                cmd = self.inQ.get()
                t1 = time.time()
                self.logger.info(f'new cmd (len={len(cmd)}), {t1-t0:0.4f}s')
                if isinstance(cmd, str):
                    self.logger.info(f'    {cmd}')
                if cmd == 'shutdown':
                    if self.currentFits is not None:
                        self.close()
                    self.logger.info(f'shutting down')
                    try:
                        self.reportSuccess('SHUTDOWN')
                    finally:
                        return
                if cmd == 'close':
                    self.close()
                    continue

                cmd, *cmdArgs = cmd
                if cmd == 'create':
                    self.logger.info(f'new cmd {cmd} (len={len(cmdArgs)})')
                    path, header = cmdArgs
                    if path is None:
                        raise ValueError("cannot open file without a path")
                    self.create(path, header)
                elif cmd == 'write':
                    hduId, extname, data, header = cmdArgs
                    t0 = time.time()
                    self.write(hduId, extname, data, header)
                    t1 = time.time()
                    self.logger.info(f'fitsio HDU write of {self.currentPath} {hduId} took {t1-t0:0.4f}s')
                else:
                    raise ValueError(f'unknown command: {cmd} {cmdArgs}')
            except Exception as e:
                self.logger.warning(f'fitsWriter command {repr(cmd)} made no sense: {e}')

                self.reportFailure('PARSING', errorDetails=f'ERROR parsing {cmd}: {e}')
                return

class FitsCatcher(threading.Thread):
    def __init__(self, caller, replyQ, name=None):
        """Run a thread to listen for progress messages from FitsWriter, forward to an interested object

        Parameters
        ----------
        caller : `object`
            object to sed progress messages to.
        replyQ : `Queue`
            where we read FitsWriter progress messages from.
        """
        if name is None:
            name = self.__class__.__name__
        threading.Thread.__init__(self, target=self.loop, name=name)
        self.logger = logging.getLogger(name)
        self.caller = caller
        self.replyQ = replyQ

    def setCaller(self, newCaller):
        self.caller = newCaller

    def loop(self):
        """Listen for progress reports from FitsWriter, forward to exposure object.

        Messages are dictionaries, all with a `command` key which we simply use to dispatch
        to the exposure owner. The message dictionaries have other keys to help processing:
        - status: "OK", or "ERROR".
        - path: the FITS pathname
        - hduId: the HDU identifier which the exposure owner sent to the FitsWriter
        - errorDetails: if status is not OK, some hopefully informative string.
        """
        while True:
            reply = self.replyQ.get()
            self.logger.info(f'reply: {reply}')
            cmd = reply['command']
            if cmd == 'CLOSE':
                self.caller.closedFits(reply)
            elif cmd == 'WRITE':
                self.caller.wroteHdu(reply)
            elif cmd == 'CREATE':
                self.caller.createdFits(reply)
            elif cmd == 'SHUTDOWN' or cmd == 'ENDCMD':
                return
            else:
                self.logger.warning(f'unknown fits writer reply: {reply}')

    def end(self):
        self.replyQ.put(dict(command='ENDCMD'))

class FitsBuffer(object):
    def __init__(self):
        """Create a process to write FITS files """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.cmdQ = Queue()
        self.replyQ = Queue()
        self.catcher = None
        self._p = Process(target=FitsWriter.setupFitsWriter, args=(self.cmdQ, self.replyQ))
        self.logger.info(f'starting FitsWriter {self._p}')
        self._p.start()

    def createFile(self, caller, path, header):
        """Create new FITS file.

        We actually create a scratch FITS file in the same directory as the final file.

        Parameters
        ----------
        caller : `object`
            Object we want to report back to, for progress made to this FITS file. Note
            that the caller can change per-FITS file, which lets us report back to the
            right command.
        path : path-like
            The full pathname of the FITS file we want to create.
        header : fitsio-compliant header
            The cards to create the PHDU with
        """

        if path is None:
            raise ValueError("must provide a pathname to create a FITS file.")
        if self.catcher is None:
            self.catcher = FitsCatcher(caller, self.replyQ)
            self.catcher.start()
        else:
            self.catcher.setCaller(caller)

        self.cmdQ.put(('create', path, header))

    def addHdu(self, data, header, hduId=1, extname='IMAGE'):
        """Add a real image HDU

        Parameters
        ----------
        data : `np.ndarray`
            The image data. Expected to be uint16 since we always RICE_1 compress.
        header : fitsio-compilant cards
            The header for this HDU
        hduId : `int`
            Some identifier for the calling object.
        extname : `str`
            What to use as the EXTNAME
        """
        self.cmdQ.put(('write', hduId, extname, data, header))

    def finishFile(self):
        """Finalize our FITS file.

        We close the fitsio object, and rename the scratch file to the final proper filename.
        """
        self.cmdQ.put(('close'))

    def shutdown(self):
        """Kill our process, etc. """
        self.cmdQ.put('shutdown')

class TestCatcher(object):
    def __init__(self, name='testCatcher'):
        """Handle and report on all messsages coming from a FitsCatcher.

        The real object would report progess keywords to MHS keywords, and do _something_ about
        errors. Not sure what it would do.

        Parameters
        ----------
        name : str, optional
            some identifying name for log output, by default 'testCatcher'
        """
        self.logger = logging.getLogger('testCatcher')
        self.logger.setLevel(logging.INFO)
        self.name = name
        self.logger.info(f'created new TestCatcher(name={name})')

    def createdFits(self, reply):
        self.logger.info(f'{self.name} createdFits: {reply}')
    def wroteHdu(self, reply):
        self.logger.info(f'{self.name} wroteHdu: {reply}')
    def closedFits(self, reply):
        self.logger.info(f'{self.name} closedFits: {reply}')
    def fitsFailure(self, reply):
        self.logger.warning(f'{self.name} fitsFailure: {reply}')

def testWrites(nfiles=5, nread=1, root='/tmp/testFits'):
    buffer = FitsBuffer()

    def makeCard(name, value, comment='no comment'):
        return dict(name=name, value=value, comment=comment)

    for fn_i in range(nfiles):
        fn = pathlib.Path(root) / f'test{fn_i}.fits'
        catcher = TestCatcher(name=f'catcher_{fn_i}')
        phdr = [makeCard('ISPHDU', True),
                makeCard('PATH', str(fn), 'the file path')]
        buffer.createFile(catcher, fn, phdr)

        for read_i in range(1, nread+1):
            im1 = np.arange(20, dtype='u2').reshape((5,4))
            im1 += 100*read_i
            hdr = [makeCard('ISPHDU', False),
                   makeCard('VAL', read_i, 'the read number')]
            extname = f'IMAGE_{read_i}' if nread > 1 else 'IMAGE'
            buffer.addHdu(im1, hdr, read_i, extname)
        buffer.finishFile()

    buffer.shutdown()

def testWriteOutput(nfiles=5, nread=1, root='/tmp/testFits'):
    for fn_i in range(nfiles):
        fn = pathlib.Path(root) / f'test{fn_i}.fits'

        try:
            ff = fitsio.FITS(fn)
        except Exception as e:
            logging.warning(f'failed to open {fn}: {e}')
            time.sleep(1)
            ff =  fitsio.FITS(fn)
        if len(ff) != nread + 1:
            raise RuntimeError(f"wrong number of reads in {fn}: {len(ff)} vs expected {nread}")
        phdu = ff[0].read_header()
        if phdu['PATH'] != str(fn):
            raise RuntimeError(f"expected correct PATH in {fn}")

        for read_i in range(1, nread+1):
            if nread == 1:
                hdu = ff['IMAGE']
            else:
                hdu = ff[f'IMAGE_{read_i}']
            hdr = hdu.read_header()
            data = hdu.read()

            if hdr['VAL'] != read_i:
                raise RuntimeError(f"expected VAL == {read_i} in {fn} HDU {read_i}, found {hdr['VAL']}")

            im1 = np.arange(20, dtype='u2').reshape((5,4))
            im1 += 100*read_i

            if np.any(data != im1):
                raise RuntimeError(f"data for {fn}, HDU {read_i} not as expected")

def main(argv=None):
    import numpy as np
    import time
    if isinstance(argv, str):
        import shlex
        argv = shlex.split(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)-20s %(levelname)-8s  %(message)s")

    ## CCD-style
    root = '/tmp/testFits_ccd'
    os.makedirs(root, mode=0o755, exist_ok=True)
    t0 = time.time()
    testWrites(root=root)
    t1 = time.time()
    testWriteOutput(root=root)
    logging.info(f'ccd writes: {t1-t0:0.4f}')

    ## HX-style
    root = '/tmp/testFits_hx'
    os.makedirs(root, mode=0o755, exist_ok=True)
    t0 = time.time()
    testWrites(nread=100, root=root)
    t1 = time.time()
    testWriteOutput(nread=100, root=root)
    logging.info(f'hx writes: {t1-t0:0.4f}')

if __name__ == "__main__":
    main()
