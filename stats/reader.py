import json
import gzip
import lzma
import os
import re

import arrow

try:
    import ujson as fast_json
except ImportError:
    fast_json = None


def decompress_open(path):
    extension = os.path.splitext(path)[1]

    if extension in ('.log', '.txt'):
        return open
    elif extension == '.xz':
        return lzma.open
    elif extension == '.gz':
        return gzip.open


def read(path):
    filename = os.path.basename(path)

    match = re.match(r'log-(\w+)-', filename)

    project = match.group(1)

    if fast_json:
        fast_loads = fast_json.loads
    else:
        fast_loads = None

    norm_loads = json.loads

    with decompress_open(path)(path, mode='rt') as file:

        for line in file:
            if fast_loads:
                try:
                    doc = fast_loads(line)
                except ValueError:
                    # May be an overflow
                    doc = norm_loads(line)
            else:
                doc = norm_loads(line)

            item = doc['item']
            nickname = doc['by']
            date = arrow.get(doc['at']).datetime
            size = sum(doc['bytes'].values())

            yield project, item, nickname, date, size
