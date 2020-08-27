import json

from contextlib import contextmanager

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL


class DatabaseManager:
    def __init__(self, db_credentials):
        self.db_credentials = db_credentials

    @contextmanager
    def get_managed_session(self):
        url = URL(**self.db_credentials)
        engine = create_engine(url)
        Session = sessionmaker(bind=engine)
        session = Session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()


with open('C:\\Users\\rober\\crusher\\db\\db_credentials\\db_credentials.json') as json_file:
    db_credentials = json.load(json_file)

dbm = DatabaseManager(db_credentials)
