import contextlib
import hashlib
import collections
from sqlalchemy import Column, Integer, String, DateTime, create_engine, insert, \
    Binary, Date, delete, select, func
import sqlalchemy.ext.declarative
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import SingletonThreadPool

DBBase = sqlalchemy.ext.declarative.declarative_base()


class LogRecord(DBBase):
    __tablename__ = 'log_records'

    id = Column(Binary, primary_key=True)
    project = Column(String, nullable=False)
    nickname = Column(String, nullable=False)
    date = Column(DateTime, nullable=False)
    size = Column(Integer, nullable=False)


class NicknameTotal(DBBase):
    __tablename__ = 'nickname_totals'

    nickname = Column(String, primary_key=True)
    size = Column(Integer, default=0, nullable=False)
    item_count = Column(Integer, default=0, nullable=False)


# TODO: daily totals by project, cumulative, etc
# class DailyTotal(DBBase):
#     __tablename__ = 'daily_totals'
#
#     date = Column(Date, primary_key=True, nullable=False)
#     project = Column(String, nullable=False)
#     size = Column(Integer, default=0, nullable=False)
#     item_count = Column(Integer, default=0, nullable=False)


class Database(object):
    def __init__(self, filename):
        def pragma_callback(connection, record):
            connection.execute('PRAGMA synchronous=NORMAL')

        self._engine = create_engine(
            'sqlite:///{0}'.format(filename), poolclass=SingletonThreadPool
        )
        sqlalchemy.event.listen(self._engine, 'connect', pragma_callback)
        DBBase.metadata.create_all(self._engine)
        self._session_maker_instance = sessionmaker(bind=self._engine)

    @contextlib.contextmanager
    def _session(self):
        session = self._session_maker_instance()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    @contextlib.contextmanager
    def insert_session(self):
        items = []

        def add_record(project, item, nickname, date, size):
            hasher = hashlib.sha1()
            hasher.update(project.encode('utf8'))
            hasher.update(b'\t')
            hasher.update(item.encode('utf8'))
            hasher.update(b'\t')
            hasher.update(nickname.encode('utf8'))
            hasher.update(b'\t')
            hasher.update(date.isoformat().encode('utf8'))

            record_id = hasher.digest()

            items.append({
                'id': record_id,
                'project': project,
                'nickname': nickname,
                'date': date,
                'size': size
            })

        yield add_record

        with self._session() as session:
            session.execute(
                insert(LogRecord).prefix_with('OR IGNORE'),
                items
            )

    def analyze(self):
        nickname_size_counter = collections.Counter()
        nickname_items_counter = collections.Counter()

        with self._session() as session:

            query = select([
                LogRecord.project, LogRecord.nickname,
                LogRecord.date, LogRecord.size])

            for counter, row in enumerate(session.execute(query)):
                nickname_size_counter[row.nickname] += row.size
                nickname_items_counter[row.nickname] += 1

                if counter % 10000 == 0:
                    print(counter)

            nickname_total_values = []

            for nickname in nickname_size_counter:
                nickname_total_values.append({
                    'nickname': nickname,
                    'size': nickname_size_counter[nickname],
                    'item_count': nickname_items_counter[nickname]
                })

            session.execute(delete(NicknameTotal))
            session.execute(insert(NicknameTotal), nickname_total_values)

    def get_totals(self):
        with self._session() as session:
            query = select([
                func.sum(NicknameTotal.size),
                func.sum(NicknameTotal.item_count)]
            )
            return session.execute(query).first()

    def get_nickname_totals(self):
        with self._session() as session:
            query = select([
                NicknameTotal.nickname,
                NicknameTotal.size,
                NicknameTotal.item_count,
            ]).order_by(NicknameTotal.size.desc())

            for row in session.execute(query):
                yield {
                    'nickname': row.nickname,
                    'size': row.size,
                    'item_count': row.item_count,
                }
