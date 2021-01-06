import os
import hashlib

import pandas as pd
from pprint import PrettyPrinter
p = PrettyPrinter(indent=4)


BLOCK_SIZE = 65536


def get_sha256(data):
    if not data:
        return ''
    return hashlib.sha256(data).hexdigest()


def get_hash_of_file(file_path):
    hash_obj = hashlib.sha256()
    with open(file_path, 'rb') as f:
        fb = f.read(BLOCK_SIZE)
        while len(fb) > 0:
            hash_obj.update(fb)
            fb = f.read(BLOCK_SIZE)
    return hash_obj.hexdigest()


def build_pipeline(pipeline):
    _previous_file_path = None

    for each in pipeline:
        if each['type'] == 'file':
            pass
        elif each['type'] == 'transform' and _previous_file_path:
            each['location'] = each['transform_func'](_previous_file_path)

        each['dsha'] = get_hash_of_file(each['location'])
        each['psha'] = ''

        _previous_file_path = each['location']

        p.pprint(each)


if __name__ == "__main__":

    def _extracted(file_path):
        df = pd.read_csv(file_path)
        df['col3'] = df['col2'].apply(lambda x: x * x)
        output_path = os.path.join(os.path.split(file_path)[0], 'extracted.csv')
        df.to_csv(output_path, index=False)
        return output_path

    def _transformed(file_path):
        df = pd.read_csv(file_path)
        df['col4'] = df['col3'].apply(lambda x: x * x)
        output_path = os.path.join(os.path.split(file_path)[0], 'transformed.csv')
        df.to_csv(output_path, index=False)
        return output_path

    _pipeline = [
        {
            'type': 'file',
            'location': '/Users/hamza/Code/hamzaahmad-io/Ontelligence-IO/sandbox/raw.csv',
        },
        {
            'type': 'transform',
            'transform_func': _extracted
        },
        {
            'type': 'transform',
            'transform_func': _transformed
        },
    ]
    build_pipeline(_pipeline)
