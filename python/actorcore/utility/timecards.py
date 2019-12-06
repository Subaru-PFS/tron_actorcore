import numpy as np
import time

import pytz
import astropy.time
import astropy.coordinates

# Until we upgrade astropy, use this hack. The config file does not work as expected.
import astropy.utils.iers as iers
iers.conf.iers_auto_url = 'ftp://cddis.gsfc.nasa.gov/pub/products/iers/finals2000A.all'

class TimeCards(object):
    """ Provide FITS time cards for Subaru. """

    def __init__(self, startTime=None, location="Subaru", timezoneName='HST'):
        """ Set the starting time of the exposure.

        Args
        ----
        startTime : `astropy.time.Time`
          The start of the exposure. Must be utc
        """

        if startTime is None:
            startTime = self._now()

        if not (isinstance(startTime, astropy.time.Time) and startTime.scale == 'utc'):
            raise TypeError("startTime must be a UTC astropy.time.Time")

        self.startTime = startTime
        self.endTime = None
        self.location = astropy.coordinates.EarthLocation.of_site(location)
        self.timezoneName = timezoneName
        self.timezone = pytz.timezone(timezoneName)

    def _now(self):
        """ Return an astropy.time.Time for now. """

        return astropy.time.Time(time.time(), format='unix', scale='utc')

    def __str__(self):
        return f"TimeCards(start={self.startTime} end={self.endTime})"

    def end(self, endTime=None, expTime=None):
        """ Set the time of the end of the exposure.

        Args
        ----
        endTime : `astropy.time.Time`
          The end of the exposure. Must be utc
        expTime : `float`
          Th exposure time.

        Only one of the two can be passed in. Or neither in which case we use now.
        """
        if (endTime is not None) and (expTime is not None):
            raise ValueError('both expTime or endTime cannot be used')

        if expTime is not None:
            dtime = astropy.time.TimeDelta(expTime, format='sec')
            endTime = self.startTime + dtime
        else:
            if endTime is None:
                endTime = self._now()
            
        if not (isinstance(endTime, astropy.time.Time) and endTime.scale == 'utc'):
            raise TypeError("endTime must be a UTC astropy.time.Time")

        self.endTime = endTime

    @staticmethod
    def dateAndTime(t):
        t.format = 'isot'
        return t.isot.split('T')

    def _getCardsForTime(self, timeStamp, suffix, timeComment=""):
        """ Given a time, generates all the Subaru time cards.

        Args
        ----
        timeStamp : `astropy.time.Time`
          The time to format for.
        suffix : `str`
          A string to append to the card names
        timeComment : `str`
          A stting to append to the card comment

        Returns
        -------
        cards : list of `fitsio.FITSCard` compliant dictionaries.

        """
        cards = []

        cards.append(dict(name=f"MJD{suffix}",
                          value=np.round(timeStamp.mjd, 8),
                          comment=f"[d] MJD {timeComment}"))

        cards.append(dict(name=f"UT{suffix}",
                          value=self.dateAndTime(timeStamp)[1],
                          comment=f"[HMS] UT {timeComment}"))

        local = timeStamp.to_datetime(timezone=self.timezone)
        localValue = local.time().isoformat(timespec='milliseconds')
        cards.append(dict(name=f"{self.timezoneName}{suffix}",
                          value=localValue,
                          comment=f"[HMS] {self.timezoneName} {timeComment}"))

        last = timeStamp.sidereal_time('apparent', self.location.lon)
        cards.append(dict(name=f"LST{suffix}",
                          value=last.to_string(sep=':', precision=3),
                          comment=f"[HMS] LST {timeComment}"))

        return cards

    def getCardsForTime(self, timePoint, addSuffix=True):
        """ Generate all the cards for the start, middle, or end of an exposure.

        Basically, arranges to call self._getCardsForTime() with appropriate card names.

        Args
        ----
        timePoint : {'start', 'middle', 'end'}
          The time we want cards for.
        addSuffix : `bool`
          If False, do not add the "-STR" to start times.

        Returns
        -------
        cards : list of `fitsio.FITSCard` compliant dictionaries.
        """
        if timePoint == 'start':
            suffix = "-STR" if addSuffix else ""
            timestamp = self.startTime
            comment = "at exposure start"
        elif timePoint == "end":
            if self.endTime is None:
                raise RuntimeError("end time must be known to generate -END or middle time cards")
            suffix = "-END"
            timestamp = self.endTime
            comment = "at exposure end"
        elif timePoint == 'middle':
            if self.endTime is None:
                raise RuntimeError("end time must be known to generate -END or middle time cards")
            suffix = ""
            timestamp = self.startTime + (self.endTime - self.startTime)/2
            comment = "at mid-exposure"
        else:
            raise ValueError("timePoint must be 'start', 'end', or ''")

        if timePoint in ('end', None) and self.endTime is None:
            raise RuntimeError("end time must be known to generate -END or middle time cards")

        return self._getCardsForTime(timestamp, suffix, comment)

    def baseCards(self):
        """ Return the common Subaru FITS time cards.

        Specifically, Subaru is always UTC, and does not want times in DATE-OBS.
        """
        cards = []

        date, _ = self.dateAndTime(self.startTime)
        cards.append(dict(name="TIMESYS", value="UT", comment="Time System used in the header"))
        cards.append(dict(name="DATE-OBS", value=date, comment="[YMD] Observation start"))

        return cards

    def getCards(self):
        """ Get all time cards for an exposure.

        We always return TIMESYS, DATE-OBS.

        We always generate cards for the start of an exposure.
        If we have an endTime, we also generate cards for the middle and end.

        Returns
        -------
        cards : list of `fitsio.FITSRecord` compliant dictionaries.

        """
        cards = self.baseCards()
        if self.endTime is None:
            cards.extend(self.getCardsForTime('start', addSuffix=False))
        else:
            cards.extend(self.getCardsForTime('start'))
            cards.extend(self.getCardsForTime('middle'))
            cards.extend(self.getCardsForTime('end'))

        return cards

def main():
    import fitsio
    
    now = astropy.time.Time('2000-01-01T01:01:01', scale='utc')
    then = now + astropy.time.TimeDelta(5.0, format='sec')
    
    timeCards = TimeCards(now)
    timeCards.end(then)

    cards = timeCards.getCards()
    hdr = fitsio.FITSHDR([fitsio.FITSRecord(c) for c in cards])
    print(hdr)
    return hdr
    
        
if __name__ == "__main__":
    main()
    
