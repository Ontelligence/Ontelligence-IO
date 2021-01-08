from typing import Optional, List, Union, Dict, Any

try:
    import pendulum
except ImportError:
    pass
from calendar import monthrange
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from operator import itemgetter
from itertools import groupby

# TODO: Convert "date_format" to Windows/Linux format.
#   date_format = date_format.replace('%-', '%#') if sys.platform == 'win32' else date_format.replace('%#', '%-')


DATE_FORMAT = '%Y-%m-%d'
try:
    TIMEZONE = pendulum.timezone('America/New_York')
except Exception:
    pass

# date_format = date_format.replace('%-', '%#') if sys.platform == 'win32' else date_format.replace('%#', '%-')


def _try_strptime(date: str, date_format: str) -> datetime:
    try:
        return datetime.strptime(date, date_format)
    except Exception:
        return datetime.strptime(date, DATE_FORMAT)


def _resolve_date_format(date: str, **kwargs):
    date_format = kwargs.get('date_format', DATE_FORMAT)
    try:
        return _try_strptime(date, date_format).strftime(date_format)
    except Exception:
        return _try_strptime(date, date_format).strftime(DATE_FORMAT)


def today(delta_days: Optional[int] = None, delta_weeks: Optional[int] = None, return_datetime: Optional[bool] = False,
          **kwargs) -> Union[str, datetime]:
    date_format = kwargs.get('date_format', DATE_FORMAT)
    try:
        date = datetime.today().astimezone(tz=TIMEZONE)
    except Exception:
        date = datetime.today()
    if delta_days:
        date += timedelta(days=delta_days)
    if delta_weeks:
        date += timedelta(weeks=delta_weeks)
    return date if return_datetime else date.strftime(date_format)


def boweek(date: Optional[str] = None, sunday_start: Optional[bool] = False, delta_weeks: Optional[int] = 0,
           return_datetime: Optional[bool] = False, **kwargs) -> Union[str, datetime]:
    date_format = kwargs.get('date_format', DATE_FORMAT)

    if date:
        date = _try_strptime(date, date_format)
    else:
        try:
            date = datetime.today().astimezone(tz=TIMEZONE)
        except Exception:
            date = datetime.today()

    date = date - timedelta(days=date.isoweekday() if sunday_start else date.weekday()) + relativedelta(weeks=delta_weeks)
    return date if return_datetime else date.strftime(date_format)


def eoweek(date: Optional[str] = None, sunday_start: Optional[bool] = False, delta_weeks: Optional[int] = 0,
           return_datetime: Optional[bool] = False, **kwargs) -> Union[str, datetime]:
    date_format = kwargs.get('date_format', DATE_FORMAT)
    date = boweek(date=date, sunday_start=sunday_start, date_format=date_format, delta_weeks=delta_weeks, return_datetime=True)
    date = date + timedelta(days=6)
    return date if return_datetime else date.strftime(date_format)


def bomonth(date: Optional[str] = None, delta_months: Optional[int] = 0, return_datetime: Optional[bool] = False, **kwargs) -> Union[str, datetime]:
    date_format = kwargs.get('date_format', DATE_FORMAT)
    if date:
        date = _try_strptime(date, date_format)
    else:
        try:
            date = datetime.today().astimezone(tz=TIMEZONE)
        except Exception:
            date = datetime.today()
    date = date.replace(day=1) + relativedelta(months=delta_months)
    return date if return_datetime else date.strftime(date_format)


def eomonth(date: Optional[str] = None, delta_months: Optional[int] = 0, return_datetime: Optional[bool] = False, **kwargs) -> Union[str, datetime]:
    date_format = kwargs.get('date_format', DATE_FORMAT)
    date = bomonth(date=date, date_format=date_format, return_datetime=True)
    date = date + relativedelta(months=delta_months)
    date = date.replace(day=monthrange(year=date.year, month=date.month)[1])
    return date if return_datetime else date.strftime(date_format)


def date_range(start_date: Union[str, datetime], end_date: Optional[Union[str, datetime]] = None,
               return_dates: Optional[bool] = False, **kwargs) -> List[Union[str, datetime]]:
    date_format = kwargs.get('date_format', DATE_FORMAT)
    if isinstance(start_date, str):
        start_date = _try_strptime(start_date, date_format)
    if end_date is None:
        end_date = datetime.now().strftime(date_format)
    if isinstance(end_date, str):
        end_date = _try_strptime(end_date, date_format)
    date_range_list = [start_date + timedelta(days=i) for i in range(0, (end_date - start_date).days + 1)]
    if return_dates:
        return date_range_list
    else:
        return [x.strftime(date_format) for x in date_range_list]


def week_range(start_date: str, end_date: Optional[str] = None, **kwargs) -> List[str]:
    weeks = [boweek(date=x, **kwargs) for x in date_range(start_date=start_date, end_date=end_date, **kwargs)]
    return list(dict.fromkeys(weeks))


def group_dates_into_intervals(date_list: List[Union[datetime, str]], return_dates: Optional[bool] = False, **kwargs) -> List[Dict[str, str]]:
    date_format = kwargs.get('date_format', DATE_FORMAT)
    date_list = sorted([_try_strptime(x, date_format) if isinstance(x, str) else x for x in date_list])
    date_intervals = []
    temp = []
    for each_date in date_range(start_date=date_list[0], end_date=date_list[-1], return_dates=True):
        if each_date in date_list:
            temp.extend([each_date])
        else:
            if temp:
                date_intervals.extend([{'start_date': temp[0], 'end_date': temp[-1]}])
                temp = []
    if temp:
        date_intervals.extend([{'start_date': temp[0], 'end_date': temp[-1]}])
    if return_dates:
        return date_intervals
    return [{'start_date': x['start_date'].strftime(date_format), 'end_date': x['end_date'].strftime(date_format)} for x in date_intervals]


def resolve_date_input(exact_date: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None,
                       **kwargs) -> Dict[str, Any]:
    if exact_date:
        start_date = _resolve_date_format(exact_date, **kwargs)
        end_date = start_date
        list_of_dates = [start_date]
    else:
        if not start_date:
            raise Exception('Please specify a "start_date"')
        start_date = _resolve_date_format(start_date, **kwargs)
        if end_date:
            end_date = _resolve_date_format(end_date, **kwargs)
        else:
            end_date = today(**kwargs)
        list_of_dates = date_range(start_date=start_date, end_date=end_date, **kwargs)
    return {'date_range': {'start_date': start_date, 'end_date': end_date}, 'list_of_dates': list_of_dates}
