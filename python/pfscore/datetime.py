from astropy import time as astroTime
import datetime
from pfs.utils.spectroIds import getSite


class MjdTime(astroTime.Time):
    timeOffset = dict(UTC=0, HST=-10)

    def __init__(self, value=None):
        if value is None:
            value = astroTime.Time.now().mjd
            astroTime.Time.__init__(self, value, format='mjd')
        else:
            astroTime.Time.__init__(self, value)

        tz = 'HST' if getSite() == 'S' else 'UTC'
        self.tz = tz

    def __new__(cls, *args):
        return astroTime.Time.__new__(cls, 0)

    def __float__(self):
        return float(self.value)

    def __str__(self):
        return f'{self.date.isoformat()}{self.tz}'

    def isoformat(self):
        return str(self)

    @property
    def timedelta(self):
        return datetime.timedelta(hours=MjdTime.timeOffset[self.tz])

    @property
    def tzinfo(self):
        return datetime.timezone(self.timedelta)

    @property
    def date(self):
        return self.datetime + self.timedelta

    @classmethod
    def fromisoformat(cls, datestr, fmt='%Y-%m-%dT%H:%M:%S.%f'):
        tz = datestr[-3:]
        datestr = datestr[:-3]
        dtime = datetime.datetime.strptime(datestr, fmt) - datetime.timedelta(hours=MjdTime.timeOffset[tz])
        return cls(dtime)


