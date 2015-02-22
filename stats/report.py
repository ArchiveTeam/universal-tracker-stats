import datetime
import os

from jinja2 import Environment, PackageLoader


def format_size(num, format_str='{num:.1f} {unit}'):
    '''Format the file size into a human readable text.

    http://stackoverflow.com/a/1094933/1524507
    '''
    for unit in ('B', 'KiB', 'MiB', 'GiB'):
        if -1024 < num < 1024:
            return format_str.format(num=num, unit=unit)

        num /= 1024.0

    return format_str.format(num=num, unit='TiB')


def format_num(num):
    return '{:,}'.format(num)


def report(database, dest_dir):
    env = Environment(
        loader=PackageLoader('stats', 'templates'),
        autoescape=True,
    )
    env.globals['enumerate'] = enumerate
    env.globals['format_size'] = format_size
    env.globals['format_num'] = format_num
    env.globals['tuple'] = tuple
    env.globals['generation_date'] = datetime.datetime.utcnow().isoformat()

    total_bytes, total_items = database.get_totals()
    nickname_infos = tuple(database.get_nickname_totals())

    index_template = env.get_template('index.html')
    index_template.stream(
        total_items=total_items,
        total_bytes=total_bytes,
        nickname_infos=nickname_infos,
    ).dump(os.path.join(dest_dir, 'index.html'))

    nickname_template = env.get_template('nickname_totals.html')
    nickname_template.stream(
        nickname_infos=nickname_infos
    ).dump(os.path.join(dest_dir, 'nickname.html'))
