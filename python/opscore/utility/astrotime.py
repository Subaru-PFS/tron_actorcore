"""
Enhancements to the standard datetime package for astronomical applications.
"""

# Created 29-Jul-2008 by David Kirkby (dkirkby@uci.edu)

from datetime import tzinfo,timedelta,datetime
from math import floor

class AstroTimeException(Exception):
    pass

class AstroTime(datetime):
    """
    Enhanced version of datetime suitable for astronomical applications.
    
    Wraps the datetime class to add support for leap-second adjustments
    specified via the timezone and conversion to/from Modified Julian
    Date formats.
    """
    # a datetime whose MJD is exactly 50000.
    mjdEpoch = datetime(1995,10,10)
    def __new__(cls,*args,**kargs):
        """
        Extends constructor to support up-casting from a datetime object.
        
        AstroTime(datetime=dt)
        AstroTime(datetime=dt,deltasecs=+33)
        """
        if (len(args) == 0 and 'datetime' in kargs and
            (len(kargs) == 1 or (len(kargs) == 2 and 'deltasecs' in kargs))):
            if not isinstance(kargs['datetime'],datetime):
                raise AstroTimeException('expect datetime instance')
            deltasecs = kargs['deltasecs'] if 'deltasecs' in kargs else 0
            dt = kargs['datetime'] + timedelta(seconds=deltasecs)
            return datetime.__new__(cls,dt.year,dt.month,dt.day,
                dt.hour,dt.minute,dt.second,dt.microsecond,dt.tzinfo)
        else:
            return datetime.__new__(cls,*args,**kargs)
    @staticmethod
    def now(tz=None):
        """Identical to datetime.now() but returns an AstroTime"""
        dt = AstroTime(datetime=datetime.now(tz))
        delta = dt.__leapseconds(tz)
        return AstroTime(datetime=dt,deltasecs=delta)
    @staticmethod
    def fromtimestamp(timestamp,tz=None):
        """Identical to datetime.fromtimestamp() but returns an AstroTime"""
        dt = AstroTime(datetime=datetime.fromtimestamp(timestamp,tz))
        delta = dt.__leapseconds(tz)
        return AstroTime(datetime=dt,deltasecs=delta)
    @staticmethod
    def combine(date,time):
        """Identical to datetime.combine() but returns an AstroTime"""
        return AstroTime(datetime=datetime.combine(date,time))
    def __leapseconds(self,tz,default=0):
        """Returns the leap-second adjustment of tz or default if none is available"""
        result = default
        try:
            result = tz.leapseconds(self)
        except AttributeError:
            pass
        return result
    def utcoffset(self):
        """Returns our offset from UTC, including any leap-second adjustments."""
        return datetime.utcoffset(self) + timedelta(seconds=self.__leapseconds(self.tzinfo))
    def utctimetuple(self):
        dt = self - timedelta(seconds=self.__leapseconds(self.tzinfo))
        return dt.utctimetuple()
    def astimezone(self,tz):
        """
        Identical to datetime.astimezone() but returns an AstroTime.
        
        Performs leap-second adjustments if necessary.
        """
        delta = self.__leapseconds(tz) - self.__leapseconds(self.tzinfo)
        return AstroTime(datetime=datetime.astimezone(self,tz),deltasecs=delta)
    def timetz(self):
        delta = self.__leapseconds(self.tzinfo,0)
        if not delta == 0:
            raise AstroTimeException('time.time does not support leap seconds')
        return datetime.timetz(self)
    def MJD(self):
        """Returns the Modified Julian Date corresponding to our date and time."""
        if self.year <= 0:
            raise AstroTimeException("MJD calculations not supported for BC dates")
        (y,m,d) = (self.year,self.month,self.day)
        jd = (367*y - floor(7*(y + floor((m+9)/12))/4) -
            floor(3*(floor((y+(m-9)/7)/100)+1)/4) + floor(275*m/9) + d + 1721028.5)
        mjd = jd - 2400000.5
        (h,m,s,us) = (self.hour,self.minute,self.second,self.microsecond)
        mjd += (h + (m + (s + us/1000000.)/60.)/60.)/24.
        return mjd
    @staticmethod
    def fromMJD(mjd,tz=None):
        """
        Returns an AstroTime initialized from an MJD value.
        
        No timezone or leap-second adjustments are made since the MJD
        value is assumed to already be in the specified time zone.
        """
        dt = AstroTime.mjdEpoch.replace(tzinfo=tz) + timedelta(days = mjd - 50000.)
        return AstroTime(datetime=dt)
    def __str__(self):
        formatted = datetime.__str__(self)
        delta = self.__leapseconds(self.tzinfo,None)
        if delta is not None:
            formatted += '%+03d' % self.tzinfo.leapseconds(self)
        formatted += ' MJD %.6f' % self.MJD()
        if self.tzname() is not None:
            formatted += ' %s' % self.tzname()
        return formatted
    def __repr__(self):
        return datetime.__repr__(self).replace('datetime.datetime',self.__class__.__name__)
        
ZERO = timedelta(0)

class CoordinatedUniversalTime(tzinfo):
    """
    A timezone class for tagging a datetime as being in UTC.
    """
    def utcoffset(self,dt):
        return ZERO
    def dst(self,dt):
        return ZERO
    def tzname(self,dt):
        return 'UTC'
    def leapseconds(self,dt):
        return int(0)

UTC = CoordinatedUniversalTime()

class InternationalAtomicTime(CoordinatedUniversalTime):
    """
    A timezone class for tagging a datetime as being in TAI and converting to/from TAI.
    
    Leapseconds are documented at ftp://maia.usno.navy.mil/ser7/tai-utc.dat
    """
    def tzname(self,dt):
        return 'TAI'
    def leapseconds(self,dt):
        """Apply leap second conversion to a UTC time.

        This should be read from some IERS/USNO data file, but we are
        hand coding the last few entries, as of 2017-01-04:

        1999 JAN  1 =JD 2451179.5  TAI-UTC=  32.0       S + (MJD - 41317.) X 0.0      S
        2006 JAN  1 =JD 2453736.5  TAI-UTC=  33.0       S + (MJD - 41317.) X 0.0      S
        2009 JAN  1 =JD 2454832.5  TAI-UTC=  34.0       S + (MJD - 41317.) X 0.0      S
        2012 JUL  1 =JD 2456109.5  TAI-UTC=  35.0       S + (MJD - 41317.) X 0.0      S
        2015 JUL  1 =JD 2457204.5  TAI-UTC=  36.0       S + (MJD - 41317.) X 0.0      S
        2017 JAN  1 =JD 2457754.5  TAI-UTC=  37.0       S + (MJD - 41317.) X 0.0      S

        """
        
        if dt.year >= 2017:
            return 37
        elif dt.year > 2015 or dt.year == 2015 and dt.month >= 7:
            return 36
        elif dt.year > 2012 or dt.year == 2012 and dt.month >= 7:
            return 35
        elif dt.year >= 2009:
            return 34
        elif dt.year >= 2006:
            return 33
        elif dt.year >= 1999:
            return 32
        else:
            raise AstroTimeException("Leap seconds not tabulated before 1999")

TAI = InternationalAtomicTime()
