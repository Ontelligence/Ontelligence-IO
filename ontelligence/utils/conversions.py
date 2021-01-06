from typing import Optional
from time import time


def s2hms(t: float, timestamp: Optional[bool] = True):
    if timestamp:
        seconds = int(round(time() - t, 0))
    else:
        seconds = t
    hours, minutes, seconds = seconds // 3600, (seconds % 3600) // 60, (seconds % 3600) % 60
    if hours > 0:
        return str('{:01}:{:02}:{:02}'.format(hours, minutes, seconds) + ' hours')
    elif minutes > 0:
        return str('{:01}:{:02}'.format(minutes, seconds) + ' minutes')
    else:
        return str('{:01}'.format(seconds) + ' seconds')


def b2gmk(b: int):
    if b < 1024 or b >= 1024**6:
        return str(round(b, 0)) + ' B'
    elif 1024 <= b < 1024**2:
        return str(round(b / 1024, 2)) + ' KB'
    elif 1024**2 <= b < 1024**3:
        return str(round(b / 1024 ** 2, 2)) + ' MB'
    elif 1024**3 <= b < 1024**4:
        return str(round(b / 1024 ** 3, 2)) + ' GB'
    elif 1024**4 <= b < 1024**5:
        return str(round(b / 1024 ** 4, 2)) + ' TB'
    elif 1024**5 <= b < 1024**6:
        return str(round(b / 1024 ** 5, 2)) + ' PB'
    else:
        return str(b)
