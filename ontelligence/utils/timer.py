import time
from ontelligence.utils.conversions import s2hms


def timer(method):
    def timed(*args, **kwargs):
        t = time.time()
        result = method(*args, **kwargs)
        if 'log_time' in kwargs:
            name = kwargs.get('log_name', method.__name__.upper())
            kwargs['log_time'][name] = int((time.time() - t) * 1000)
        else:
            print('TIMER: [{}] completed in {}.\n'.format(method.__name__, s2hms(t)))
        return result
    return timed
