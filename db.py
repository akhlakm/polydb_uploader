import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import insert, update

import pylogg
log = pylogg.New('db')

try:
    from sshtunnel import SSHTunnelForwarder
    _remote_access = True
except ImportError:
    _remote_access = False


# declare our own base class that all of the modules in orm can import
class Operation:
    def __init__(self, table : DeclarativeBase):
        self.table : DeclarativeBase = table

    def serialize(self):
        """ Serialize the data """
        res = {}
        for attr in vars(self.table.__class__):
            if attr.startswith("_"):
                continue
            val = self.table.__getattribute__(attr)
            res[attr] = val
        return res

    def get_one(self, session, criteria = {}) -> DeclarativeBase:
        """ Get the first element from current table using a criteria ."""
        return session.query(self.table.__class__).filter_by(**criteria).first()

    def get_all(self, session, criteria = {}) -> list[DeclarativeBase]:
        """ Get all the elements from current table using a criteria ."""
        return session.query(self.table.__class__).filter_by(**criteria).all()

    def insert(self, session, *, test=False):
        payload = self.serialize()
        try:
            session.execute(insert(self.table.__class__), payload)
        except Exception as err:
            log.error("Insert ({}) - {}", self.table.__tablename__, err)
        if test:
            session.rollback()
            log.trace("Insert ({}) - rollback", self.table.__tablename__)
        else:
            session.commit()

    def update(self, session, newObj, *, test=False):
        values = newObj.serialize()
        try:
            sql = update(self.table.__class__).where(self.id == newObj.id).values(**values)
            self.session.execute(sql)
        except Exception as err:
            log.error("Update ({}) - {}", self.table.__tablename__, err)
        if test:
            session.rollback()
            log.trace("Update ({}) - rollback", self.table.__tablename__)
        else:
            session.commit()

    def upsert(self, session, which: dict, payload, name : str, *,
               update=False, test=False) -> DeclarativeBase:
        """
        Update the database by inserting or updating a record.
        Args:
            which dict:     The criteria to check if the record already exists.
            payload:        The object to insert to the table.
            name str:       Name or ID of the object, for logging purposes.
            update bool:    Whether to update the record if already exists.
        """

        table = self.table.__tablename__

        # select existing record by "which" criteria
        x = self.get_one(session, which)

        # set the foreign keys
        payload.__dict__.update(which)

        if x is None:
            self.insert(session, test=test)
            log.trace(f"{self.table.__tablename__} add: {name}")
        else:
            if update:
                self.update(session, x, payload, test=test)
                log.trace(f"{self.table.__tablename__} update: {name}")
            else:
                log.trace(f"{self.table.__tablename__} ok: {name}")

        return self.get_one(session, which)


class Frame:
    """ Iteratively build a dictionary for a dataframe.
    """
    def __init__(self, columns = None) -> None:
        self._tabl = {}
        self._cols = columns

    def _setup_columns(self):
        """ Initialize the dictionary items. """
        for key in self._cols:
            if not key in self._tabl:
                self._tabl[key] = []

    def contains(self, column, value):
        """ Check if a value already exists in a column. """
        if column in self._tabl.keys():
            return value in self._tabl[column]
        else: return False
    
    def pad_columns(self):
        """ Make sure all columns are of same size.
            Add NA to pad the shorter columns.
        """
        max_len = 0
        for key in self._cols:
            col_len = len(self._tabl[key])
            if col_len > max_len:
                max_len = col_len

        for key in self._cols:
            col_len = len(self._tabl[key])
            while col_len < max_len:
                self._tabl[key].append(None)
                col_len = len(self._tabl[key])

    def add(self, **kwargs):
        if self._cols is None:
            self._cols = kwargs.keys()

        self._setup_columns()

        for key in kwargs:
            value = kwargs[key]
            if key in self._cols:
                self._tabl[key].append(value)

    @property
    def df(self):
        return pd.DataFrame(self._tabl)


def _setup_proxy() -> SSHTunnelForwarder | None:
    """SSH server to connect to the database through"""
    if _remote_access and len(os.getenv("SSH_TUNNEL_HOST", "")) > 0:
        server = SSHTunnelForwarder(
            (
                os.environ.get("SSH_TUNNEL_HOST"),
                int(os.environ.get("SSH_TUNNEL_PORT")),
            ),
            ssh_username=os.environ.get("SSH_USERNAME"),
            ssh_password=os.environ.get("SSH_PASSWORD"),
            remote_bind_address=(
                os.environ.get("DB_HOST"),
                int(os.environ.get("DB_PORT")),
            ),
        )
        server.start()
        log.note("SSH tunnel established.")
        return server
    else:
        return None


def _setup_engine(*, server : SSHTunnelForwarder = None, db_url = None):
    if db_url is None:
        if server is None:
            db_url = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
                os.environ.get("DB_USER"),
                os.environ.get("DB_PASSWORD"),
                os.environ.get("DB_HOST"),
                os.environ.get("DB_PORT"),
                os.environ.get("DB_NAME"))
        else:
            db_url = "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
                os.environ.get("DB_USER"),
                os.environ.get("DB_PASSWORD"),
                server.local_bind_host,
                server.local_bind_port,
                os.environ.get("DB_NAME"))
    engine = create_engine(db_url)
    log.trace("DB engine created.")
    return engine


def _new_session(engine) -> scoped_session:
    connection = engine.connect()
    session = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=connection)
    )
    log.trace("DB connected.")
    return session

eng = None
ssh = None
sess = None

def connect():
    global ssh, eng, sess
    if ssh is None:
        ssh = _setup_proxy()
    if eng is None:
        eng = _setup_engine(server=ssh)
    if sess is None:
        sess = _new_session(eng)
    return sess


def disconnect():
    global ssh, eng, sess
    if sess is not None:
        sess.close()
    if ssh is not None:
        ssh.stop()
    log.info("DB disconnect.")

