from datetime import datetime, timedelta


class datetime_ns:
    current_year = datetime.now().year

    def __init__(self, dt, ns=None, year=None):
        self.datetime = dt

        if ns is not None:
            self.ns = ns
        else:
            self.ns = 0

        if year is None:
            year = datetime_ns.current_year
        elif not isinstance(year, (int, float)):
            raise RuntimeError("Type {0} is invalid for optional argument year".format(type(year)))
        self.year = year

    @property
    def to_ns(self):
        """ As timedelta_ns in ns since start of year (hs utc)
        """
        return (self - year_start).to_ns

    @property
    def to_hsutc(self):
        """ As HitSpool utc_time timestamp (hs utc)
        """
        return (self - year_start).to_ns * 10

    @staticmethod
    def from_hsutc(utc_time, year=None):
        """ Create datetime_ns from HitSpool utc_time using current year
        """
        ns = int(utc_time % 1e10) // 10
        delta = timedelta(seconds=utc_time // 1e10)
        if year is None:
            year = datetime_ns.current_year
        elif not isinstance(year, (int, float)):
            raise RuntimeError("Type {0} is invalid for optional argument year".format(type(year)))
        return datetime_ns(datetime(year, 1, 1) + delta, ns)

    def __repr__(self):
        return '{0}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}.{6:09f}'.format(
            self.datetime.year, self.datetime.month, self.datetime.day,
            self.datetime.hour, self.datetime.minute, self.datetime.second, self.ns
        )

    def __add__(self, other):
        if isinstance(other, timedelta):
            return datetime_ns(self.datetime + other, self.ns)
        elif not isinstance(other, timedelta_ns):
            raise TypeError('Expected timedelta or timedelta_ns, got {0}'.format(type(other)))

        new_datetime = self.datetime + other.timedelta
        new_ns = self.ns + other.delta_ns
        if new_ns > 1e9:
            new_ns -= 1e9
            new_datetime += timedelta(seconds=1)
        return datetime_ns(new_datetime, new_ns)

    def __sub__(self, other):
        """ Override subtraction method, expects argument other to be, datetime, datetime_ns, timedelta or timedelta_ns
        """
        if isinstance(other, timedelta):
            return datetime_ns(self.datetime - other, self.ns)
        elif not isinstance(other, (timedelta_ns, datetime_ns)):
            raise TypeError('Expected timedelta or timedelta_ns, got {0}'.format(type(other)))

        if isinstance(other, timedelta_ns):
            new_datetime = self.datetime - other.timedelta
            new_ns = self.ns - other.delta_ns
            if new_ns < 0:
                new_ns += 1e9
                new_datetime -= timedelta(seconds=1)
            elif new_ns > 1e9:
                new_ns -= 1e9
                new_datetime += timedelta(seconds=1)
            return datetime_ns(new_datetime, new_ns)
        else:  # Assumes previous statements will catch anything that it not allowed
            return timedelta_ns(self.datetime - other.datetime, self.ns - other.ns)

    def __eq__(self, other):
        return self.datetime == other.datetime and self.ns == other.ns

    def __gt__(self, other):  # Self is greater than other if self occurs after other chronologically
        return self.datetime > other.datetime or (self.datetime == other.datetime and self.ns > other.ns)

    def __ge__(self, other):
        return self > other or self == other

    def __lt__(self, other):  # Self is less than other if self occurs before other chronologically
        return self.datetime < other.datetime or (self.datetime == other.datetime and self.ns < other.ns)

    def __le__(self, other):
        return self < other or self == other


class timedelta_ns:

    def __init__(self, tdelta=timedelta(0), ns=0):
        self.timedelta = tdelta
        self.delta_ns = ns

    def __repr__(self):
        return 'datetime_ns.timedelta_n(seconds={0}, ns={1})'.format(self.timedelta.total_seconds(), self.delta_ns)

    @property
    def to_ns(self):
        return int(self.timedelta.total_seconds() * 1e9 + self.delta_ns)

    @classmethod
    def from_ns(cls, ns_time):
        ns = int(ns_time % 1e9)
        delta = timedelta(seconds=ns_time // 1e9)
        return cls(delta, ns)

    @classmethod
    def from_hms(cls, hours=0, minutes=0, seconds=0, ns=0):
        return cls(timedelta(hours=hours, minutes=minutes, seconds=seconds), ns)

# TODO: Figure out how to do this in a class/instance specific way!
year_start = datetime_ns(datetime(datetime.now().year, 1, 1), 0)
