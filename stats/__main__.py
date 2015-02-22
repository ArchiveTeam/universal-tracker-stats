import argparse

import stats.database
import stats.reader
import stats.report


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--database')
    subparsers = arg_parser.add_subparsers()

    load_parser = subparsers.add_parser('load')
    load_parser.set_defaults(func=load)
    load_parser.add_argument('file', nargs='+')

    analyze_parser = subparsers.add_parser('analyze')
    analyze_parser.set_defaults(func=analyze)

    report_parser = subparsers.add_parser('report')
    report_parser.add_argument('dest_dir')
    report_parser.set_defaults(func=report)

    args = arg_parser.parse_args()

    database = stats.database.Database(args.database)

    args.func(database, args)


def load(database, args):
    counter = 0

    for filename in args.file:
        with database.insert_session() as add_record:
            for data in stats.reader.read(filename):
                project, item, nickname, date, size = data
                add_record(project, item, nickname, date, size)
                counter += 1

                if counter % 1000 == 0:
                    print(counter)


def analyze(database, args):
    database.analyze()


def report(database, args):
    stats.report.report(database, args.dest_dir)


if __name__ == '__main__':
    main()
