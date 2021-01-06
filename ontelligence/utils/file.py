"""
Examples of how to read files:
https://stackabuse.com/reading-files-with-python/

List of hex signatures:
# Reference: https://en.wikipedia.org/wiki/List_of_file_signatures

"""
import os
import re

import csv
import gzip
import shutil
import tarfile
import binascii
import filecmp
from _io import TextIOWrapper
from typing import Generator, TypeVar, List, Optional

import pandas as pd
from cchardet import UniversalDetector
from botocore.response import StreamingBody

from ontelligence.utils.date import today, resolve_date_input


COMPRESSION_TYPES = ['.zip', '.7z', '.gz', 'tar.gz', '.bz2']

FILE_FORMATS = {
    'GZIP Archive File': {
        'extensions': ['.gz', '.gzip'],
        'signature': {'hex': '1f8b08', 'offset': 0}
    },
    '7-ZIP Compressed File': {
        'extensions': ['.7z', '.7zip'],
        'signature': {'hex': '377abcaf271c', 'offset': 0}
    },
    'Tape Archive File': {
        'extensions': ['.tar'],
        'signature': {'hex': '7573746172', 'offset': 257}
    },
    'ZIP': {
        'extensions': ['.zip'],
        'signature': {'hex': '504b0304', 'offset': 0}
    },
    'Microsoft Office Compound Binary File': {
        'extensions': ['.xls', '.doc', '.ppt'],
        'signature': {'hex': 'd0cf11e0a1b11ae1', 'offset': 0}
    },
    'Microsoft Office Open XML File': {
        'extensions': ['.xlsx', '.docx', '.pptx'],
        'signature': {'hex': '504b030414000600', 'offset': 0}
    }
}

HEX_SIGNATURES = {
    '.gz':       '1f8b08',
    # '.tar.gz':   '1f8b',
    '.7z':       '377abcaf271c',
    '.tar':      '7573746172',
    '.zip':      '504b0304',
    '.xls':      'd0cf11e0a1b11ae1',
    '.xlsx':     '504b030414000600'
}


# TODO: os.path vs pathlib?
# TODO: How to efficiently use glob?
# TODO: Handle file objects on local disk and streaming bodies from cloud storage.
# TODO: How do you read a file once and analyze it multiple times?
# TODO: How do you apply multiple complex transformations simultaneously?


class FileObjectMixin:

    file_path = None
    file_obj = None
    is_streaming_body = None
    extension = None
    dialect = None

    sample_size = 1024 ** 2 * 10  # 10MB

    def __init__(self, file_path_or_buffer):
        if isinstance(file_path_or_buffer, str):
            self.file_path = file_path_or_buffer
        elif isinstance(file_path_or_buffer, StreamingBody):
            self.is_streaming_body = True
            self.file_obj = file_path_or_buffer

    # Reference.

    def folder(self):
        raise NotImplementedError

    def name(self):
        raise NotImplementedError

    def get_extension(self):
        if not self.extension:
            _file_path, ext = os.path.splitext(self.file_path)
            if os.path.splitext(_file_path)[1]:
                ext = os.path.splitext(_file_path)[1] + ext
            self.extension = ext
        return self.extension

    # IO

    def _open(self, *args, **kwargs):
        _format = kwargs.get('format')
        if _format in ['.gz'] or self.get_extension().endswith('.gz'):
            return gzip.open(*args, **kwargs)
        elif _format in ['.tar', '.tar.gz'] or any(self.get_extension().endswith(x) for x in ['.tar', '.tar.gz']):
            return tarfile.open(args[0])
        return open(*args, **kwargs)

    def _read(self, obj, *args, **kwargs):
        if isinstance(obj, gzip.GzipFile):
            return obj.read(*args, **kwargs)
        elif isinstance(obj, tarfile.TarFile):
            for each_member in obj.getmembers():
                with obj.extractfile(each_member) as m:
                    return m.read(*args, **kwargs)
        return obj.read(*args, **kwargs)

    def _read_sample(self):
        if self.is_streaming_body:
            sample = self.file_obj.read(self.sample_size).decode('utf-8')
        else:
            with self._open(self.file_path) as f:
                sample = self._read(f, self.sample_size)
                if not isinstance(f, TextIOWrapper):
                    sample = sample.decode('utf-8')
        return sample

    # Detection.

    def get_size(self):
        if self.is_streaming_body:
            raise NotImplementedError
        return os.stat(self.file_path).st_size

    def get_created_date(self):
        raise NotImplementedError

    def get_modified_date(self):
        raise NotImplementedError

    def get_format(self):
        raise NotImplementedError

    def get_compression(self):
        raise NotImplementedError

    def get_encryption(self):
        raise NotImplementedError

    @staticmethod
    def _guess_encoding(file_obj):
        detector = UniversalDetector()
        if isinstance(file_obj, tarfile.TarFile):
            file_obj = file_obj.extractfile(file_obj.getmembers()[0]).readlines()
        for line in file_obj:
            detector.feed(line)
            if detector.done:
                break
        detector.close()
        return detector.result

    def guess_encoding(self):
        if self.is_streaming_body:
            result = self._guess_encoding(self.file_obj)
        else:
            with self._open(self.file_path, 'rb') as f:
                result = self._guess_encoding(f)
        return result.get('encoding')

    def guess_dialect(self):
        if not self.dialect:
            _sample = self._read_sample()
            self.dialect = csv.Sniffer().sniff(sample=_sample)
        return self.dialect

    # def guess_delimiter(self):
    #     raise NotImplementedError
    #
    # def guess_quotechar(self):
    #     raise NotImplementedError

    def has_headers(self) -> bool:
        _sample = self._read_sample()
        return csv.Sniffer().has_header(sample=_sample)

    def get_headers(self, delimiter=',', skip_rows=0, compression=None) -> List[str]:
        if isinstance(self.file_path, (StreamingBody, gzip.GzipFile)):
            compression = None
        try:
            data = pd.read_csv(self.file_path, sep=delimiter, skiprows=skip_rows, compression=compression, nrows=10, low_memory=False)
            return data.columns.tolist()
        except ValueError as e:
            print(f'Error: {str(e)} {os.path.split(self.file_path)[-1]}')
            raise e

    def get_row_count(self) -> int:
        raise NotImplementedError

    def get_column_count(self) -> int:
        raise NotImplementedError

    def infer_file_format(self):
        with open(self.file_path, 'rb') as f:
            signature = binascii.hexlify(f.read(300)).decode()
            print(signature)
            print([(key, val) for key, val in HEX_SIGNATURES.items() if signature.startswith(val) and key in self.extension])
        with self._open(self.file_path, 'rb') as f:
            signature = binascii.hexlify(f.read(16)).decode()
            print(signature)
            print([(key, val) for key, val in HEX_SIGNATURES.items() if signature.startswith(val) and key in self.extension])

        file_type = [(key, val) for key, val in HEX_SIGNATURES.items() if signature.startswith(val) and key in self.extension]
        return file_type[0] if file_type else self.get_extension().lower()

    # Breakdown.

    def split(self):
        raise NotImplementedError

    def combine(self):
        raise NotImplementedError

    # Transformation.

    def compress(self, compression: Optional[str] = 'GZIP'):
        if compression == 'GZIP':
            with open(self.file_path, 'rb') as f_in:
                new_file_path = f_in.name + '.gz'
                with gzip.open(new_file_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        return new_file_path

    def uncompress(self):
        raise NotImplementedError

    def encrypt(self):
        raise NotImplementedError

    def decrypt(self):
        raise NotImplementedError

    def convert_encoding(self):
        raise NotImplementedError


class File(FileObjectMixin):

    profile = None

    def __init__(self, file_path_or_buffer):
        super().__init__(file_path_or_buffer=file_path_or_buffer)

    def analyze(self):
        # File Object
        # self.profile['extension'] = self.file.get_extension()
        # self.profile['format'] = self.file.infer_file_type()
        # self.profile['encrypted'] = None
        # self.profile['compression'] = None
        # self.profile['size'] = self.file.get_size()

        # Dialect
        # self.profile['encoding'] = self.file.guess_encoding()
        # self.profile['delimiter'] = self.file.guess_dialect().delimiter
        # self.profile['doublequote'] = self.file.guess_dialect().doublequote
        # self.profile['quotechar'] = self.file.guess_dialect().quotechar
        # self.profile['skipinitialspace'] = self.file.guess_dialect().skipinitialspace
        # self.profile['lineterminator'] = self.file.guess_dialect().lineterminator
        # self.profile['quoting'] = self.file.guess_dialect().quoting

        # Content
        # self.profile['has_header'] = self.file.has_header()
        raise NotImplementedError

    def get_profile(self):
        if not self.profile:
            self.analyze()
        return self.profile

    def describe(self):
        if not self.profile:
            self.analyze()
        from pprint import PrettyPrinter
        p = PrettyPrinter(indent=4)
        p.pprint(self.profile)

    def standardize(self):
        raise NotImplementedError


class Folder:

    def __init__(self, path):
        raise NotImplementedError

    def get_size(self):
        raise NotImplementedError

    def get_files(self):
        raise NotImplementedError

    def search_files(self):
        raise NotImplementedError


# TODO: REMOVE EVERYTHING BELOW HERE.


def get_clean_headers(headers, clean_headers=True, for_query=False, join_char=None):
    invalid_characters = '!#%&’()*+,-./:;<=>?@[]^~$'
    if clean_headers:
        if join_char:
            headers = [''.join(map(lambda x:join_char if x in invalid_characters else x, col)) for col in headers]
        else:
            headers = [''.join([y for y in x if y not in invalid_characters]) for x in headers]

        headers = [x.lstrip('0123456789_').strip(' ').replace(' ', '_').lower() for x in headers]
        headers = [x if x else 'col{}'.format(i) for i, x in enumerate(headers)]

    if for_query:
        # reserved_words = [r.strip().lower() for r in rs_reserved_words]
        # headers = [('"' + x + '"') if x in reserved_words else x for x in headers]
        reserved_words = []
        headers = [('"' + x + '"') for x in headers]

    return headers


T = TypeVar('T')


def chunks(items: List[T], chunk_size: int) -> Generator[List[T], None, None]:
    """Yield successive chunks of a given size from a list of items"""
    if chunk_size <= 0:
        raise ValueError('Chunk size must be a positive integer')
    for i in range(0, len(items), chunk_size):
        yield items[i: i + chunk_size]


# TODO: Without indicators
# TODO: With date indicator
# TODO: With timestamp indicator
# TODO:
# TODO:


DATE_INDICATOR = '{DATE}'
TIMESTAMP_INDICATOR = '{TIMESTAMP}'
_ALL_INDICATORS = [DATE_INDICATOR, TIMESTAMP_INDICATOR]


def get_matching_files(files: List[str], regex: Optional[str] = None, **kwargs) -> List[str]:
    # TODO: How do you handle multiple files delivered on the same day (marked with TIMESTAMP)?

    matched_files = []
    _regex_patterns = []
    regex = regex or '.*'
    if any(x in regex for x in _ALL_INDICATORS):
        if DATE_INDICATOR in regex:
            resolved_dates = resolve_date_input(**kwargs)
            _regex_patterns.extend([regex.replace(DATE_INDICATOR, x) for x in resolved_dates['list_of_dates']])
        if TIMESTAMP_INDICATOR in regex:
            raise NotImplementedError
    else:
        _regex_patterns = [regex]

    for each_pattern in _regex_patterns:
        matched_files.extend([x for x in files if re.match(pattern=each_pattern, string=x, flags=re.IGNORECASE)])
    return matched_files


def get_latest_file(files: List[str], regex: Optional[str] = None, lookback_window: Optional[int] = 14, **kwargs) -> str:
    kwargs['start_date'] = today(delta_days=-lookback_window)
    matched_files = get_matching_files(files=files, regex=regex, **kwargs)
    if not matched_files:
        raise Exception('Could not find the latest file. Try increasing the "lookback_window" value')
    return matched_files[-1]
