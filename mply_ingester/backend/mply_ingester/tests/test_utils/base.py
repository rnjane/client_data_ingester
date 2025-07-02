import os
import unittest
from sqlalchemy import text
from mply_ingester.backend.config import ConfigBroker


def init_db(session, sql_path):
    """
    Drops all tables and runs the init SQL script to create a clean schema.
    """
    connection = session.connection()
    # Drop all tables (works for SQLite and most DBs)
    # For SQLite, pragma foreign_keys=off is needed for drop order
    connection.execute(text('PRAGMA foreign_keys=OFF;'))
    # Get all table names
    tables = connection.engine.table_names()
    for table in tables:
        connection.execute(text(f'DROP TABLE IF EXISTS {table};'))
    connection.execute(text('PRAGMA foreign_keys=ON;'))
    # Run the init SQL script
    with open(sql_path, 'r') as f:
        sql = f.read()
    for statement in filter(None, map(str.strip, sql.split(';'))):
        if statement:
            connection.execute(text(statement))
    session.commit()


class DBTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Get config path from environment or use default
        config_path = os.environ.get('INGESTER_CONFIG', os.path.expanduser('~/cfg/ingester_config.py'))
        cls.config = ConfigBroker([config_path])
        session = cls.config.get_session()
        sql_path = os.path.join(os.path.dirname(__file__), '../../db/migrations/000/000_up_init.sql')
        sql_path = os.path.abspath(sql_path)
        init_db(session, sql_path)
        session.close()
        cls.session = None  # For subclasses to use if needed

    @classmethod
    def tearDownClass(cls):
        pass

