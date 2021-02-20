import gzip

import pandas as pd
from pandas.api.types import infer_dtype as pd_infer_dtype
from botocore.response import StreamingBody

from ontelligence.core.schemas.data import Column
from ontelligence.utils.file import File


class UnknownDtypeException(Exception):  # TODO: Inherit from BaseOntelligenceException?
    pass


class BaseDataType:
    pass


class Boolean(BaseDataType):
    def __repr__(self):
        # return 'Boolean()'
        return 'BOOLEAN'


class String(BaseDataType):
    def __init__(self, size=None):
        self.size = size

    def __repr__(self):
        # return 'String()'
        return 'STRING'


class Integer(BaseDataType):
    def __repr__(self):
        # return 'Integer()'
        return 'INTEGER'


class Float(BaseDataType):
    def __repr__(self):
        # return 'Float()'
        return 'FLOAT'


class DateTime(BaseDataType):
    # TODO: Add time_zone.
    def __init__(self, timestamp=False):
        self.timestamp = timestamp
        # self.time_zone = None

    def __repr__(self):
        # return 'DateTime()'
        if self.timestamp:
            return 'DATETIME'
        return 'DATE'


class Binary(BaseDataType):
    def __repr__(self):
        return 'Binary()'


def get_string_size(str_length, round_up=False):
    if round_up:
        rounded_max_length = str_length if str_length % 100 == 0 else str_length + 100 - str_length % 100
        if rounded_max_length - str_length < 10:
            rounded_max_length += 100
        return rounded_max_length
    else:
        return str_length


def infer_dtype(values: pd.Series, round_up: bool = False):

    if not list(values):
        return String()  # Empty

    dtype = pd_infer_dtype(values, skipna=False)

    if dtype.lower() in ['categorical', 'mixed', 'mixed-integer', 'object', 'string']:
        _df = pd.to_datetime(values.values, errors='coerce')

        if _df.isnull().any() or _df.dtype in ['object'] or values.str.match(r'^[0-9]+:[0-9]+:?[0-9]*\.?[0-9]*').any():
            max_length = len(max([str(x) for x in values], key=len))
            return String(size=get_string_size(max_length, round_up=round_up))  # 'VARCHAR({})'.format(get_string_size(max_length))
        else:
            dtype = 'date' if _df.normalize().equals(_df) else 'datetime'

    if dtype.lower() in ['bool', 'boolean']:
        return Boolean()

    elif dtype.lower() in ['date', 'datetime64']:
        return DateTime()  # Date

    elif dtype.lower() in ['datetime64', 'datetime']:
        return DateTime(timestamp=True)  # Timestamp

    elif dtype.lower() in ['int64', 'integer']:
        max_val = max(abs(values))
        if max_val <= 32767:
            return Integer()  # SMALLINT
        elif max_val <= 2147483647:
            return Integer()  # INTEGER
        elif max_val <= 9223372036854775807:
            return Integer()  #BIGINT
        else:
            max_val_length = len(str(max_val))
            return String(size=get_string_size(max_val_length, round_up=round_up))  # 'VARCHAR({})'.format(get_string_size(max_val_length))

    elif dtype.lower() in ['decimal', 'float64', 'floating', 'mixed-integer-float']:
        return Float()  # FLOAT8

    raise UnknownDtypeException(f'Unmapped data type: {dtype}')


def infer_data_schema(file_path, delimiter=',', override_dtypes=None, columns=None, compression=None):
    override_dtypes = override_dtypes if override_dtypes else dict()
    dtypes = []

    if isinstance(file_path, (StreamingBody, gzip.GzipFile)):
        # compression flag ommited below because pandas.read_csv only uses compression for on-disk data as per docs
        file_path = pd.read_csv(filepath_or_buffer=file_path, delimiter=delimiter)

    if isinstance(file_path, pd.DataFrame):
        columns = list(file_path.columns)
        for column in columns:
            if override_dtypes and column.lower() in [x.lower() for x in list(override_dtypes.keys())]:
                dtypes.extend([override_dtypes[column]])
            else:
                dtypes.extend([infer_dtype(file_path[column])])
        return [Column(name=x, dtype=str(y)) for x, y in zip(columns, dtypes)]

    # columns = columns if columns else File(file_path).get_headers(delimiter=delimiter, compression=compression)
    if not columns:
        _file = File(file_path)
        columns = _file.get_headers(delimiter=delimiter, compression=compression)

    for column in columns:
        if override_dtypes and column.lower() in [x.lower() for x in list(override_dtypes.keys())]:
            dtypes.extend([override_dtypes[column]])
        else:
            values = pd.DataFrame()
            reader = pd.read_csv(file_path, sep=delimiter, chunksize=10000, usecols=[column],
                                 infer_datetime_format=True, low_memory=False, compression=compression)

            for chunk in reader:
                chunk.dropna(axis=0, inplace=True)
                values = pd.concat([values, chunk], ignore_index=True)
                values.drop_duplicates(inplace=True)
            dtypes.extend([infer_dtype(values[column])])
    # return columns, dtypes
    return [Column(name=x, dtype=str(y)) for x, y in zip(columns, dtypes)]
