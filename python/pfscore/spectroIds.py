import os
import socket

def idFromHostname(hostname=None):
    """ Return the module or cryostat id by looking at the hostname. 

    Returns:
    id : str
      Either "smM" or "[brmn]N"
    """

    if hostname is None:
        hostname = socket.gethostname()
    hostname = os.path.splitext(hostname)[0]
    try:
        _, hostid = hostname.split('-')
    except:
        return hostname
    
    return hostid

def getSite():
    """ Return the site name. Extracted from a DNS TXT record. """

    import dns.resolver
    try:
        ans = dns.resolver.query('pfs-site.', 'TXT')
    except dns.resolver.NXDOMAIN:
        raise RuntimeError('no "pfs-site." record found in DNS')

    site = ans[0].strings[0].decode('latin-1')

    return site

class SpectroIds(object):
    validArms = dict(b=1, r=2, n=3, m=4)
    validSites = {'A','J','L','S','X'}
    validModules = tuple(range(1,5))
    
    def __init__(self, partName=None, hostname=None, site=None):
        """ Track, and optionally detect, spectrograph/camera names and ids. 

        Args
        ----
        partName : str
            A name of the form 'b2' or 'sm3'.
            None to autodetect the hostname
        site : str
            One of the PFS fite names.
            None to autodetect using $PFS_SITE
       
        Internally we track the spectrograph module number, the site, and (optionally) the arm letter.
        """
        
        self.arm = None
        
        if site is None:
            site = getSite()

        if site not in self.validSites:
            raise RuntimeError('site (%s) must one of: %s' % (site, self.validSites))
        self.site = site

        if self.site == 'J':
            self.validModules += (9,)
            
        if partName is None:
            partName = idFromHostname(hostname=hostname)
            
        if len(partName) == 2:
            if partName[0] not in self.validArms:
                raise RuntimeError('arm (%s) must one of: %s' % (partName[0], list(self.validArms.keys())))
            self.arm = partName[0]
        elif len(partName) == 3:
            if partName[:2] != 'sm':
                raise RuntimeError('module (%s) must be smN' % (partName))
        else:
            raise RuntimeError('part name (%s) must be of the form "r1" or "sm2"' % (partName))

        try:
            self.specNum = int(partName[-1])
        except ValueError:
            raise RuntimeError('spectrograph number (%s) must be an integer' % (partName[-1]))
        if self.specNum not in self.validModules:
            raise RuntimeError('spectrograph number (%s) must be in %s' % (self.specNum,
                                                                           self.validModules))

    def __str__(self):
        return "SpectroIds(site=%s cam=%s arm=%s spec=%s)" % (self.site,
                                                              self.cam, self.arm, self.specNum)

    @property
    def camName(self):
        if self.arm is None:
            return None
        else:
            return "%s%d" % (self.arm, self.specNum)

    @property
    def specName(self):
        return f'sm{self.specNum}'

    @property
    def armNum(self):
        return self.validArms[self.arm]
        
    @property
    def idDict(self):
        """ Return a dictionary of our valid fields. Intended for name interpolation, etc. """
        _idDict = dict(specName=self.specName,
                       specNum=self.specNum,
                       site=self.site)

        if self.arm is not None:
            _idDict['arm'] = self.arm
            _idDict['armNum'] = self.armNum
            _idDict['camName'] = self.camName
            
        return _idDict

    def makeFitsName(self, visit, fileType, extension='.fits'):
        """ Create a complete filename 

        Args
        ====

        visit : int
          Between 0..999999
        fileType : str 
          'A' or 'B', for now.
        """

        if not isinstance(visit, int) or visit < 0 or visit > 999999:
            raise ValueError('visit must be an integer between 0 and 999999')
        if fileType not in {'A', 'B'}:
            raise ValueError('fileType must be A or B')

        if self.arm is None:
            raise RuntimeError('cannot generate a filename without an arm')
        
        return "PF%1s%1s%06d%1d%1d%s" % (self.site,
                                         fileType,
                                         visit,
                                         self.specNum,
                                         self.armNum,
                                         extension)
