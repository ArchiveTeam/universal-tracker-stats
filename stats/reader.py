import json
import lzma
import os
import re

import arrow


def decompress_open(path):
    extension = os.path.splitext(path)[1]

    if extension in ('.log', '.txt'):
        return open
    elif extension == '.xz':
        return lzma.open


def read(path):
    filename = os.path.basename(path)

    match = re.match(r'log-(\w+)-', filename)

    project = match.group(1)

    with decompress_open(path)(path, mode='rt') as file:
        for line in file:
            doc = json.loads(line)

            item = doc['item']
            nickname = doc['by']
            date = arrow.get(doc['at']).datetime
            size = sum(doc['bytes'].values())

            yield project, item, nickname, date, size
