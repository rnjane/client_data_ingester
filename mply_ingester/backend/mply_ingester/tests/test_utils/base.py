import logging
import os
import subprocess
from os.path import dirname, join

import unittest

import dbmig
from sqlalchemy import text
from sqlalchemy.engine.url import make_url

import mply_ingester
from mply_ingester.config import ConfigBroker, CONFIG_FILE_ENV_VAR

logger = logging.getLogger(__name__)


DOCKER_COMPOSE_FILE_PATH = join(dirname(mply_ingester.__file__), '../../docker/docker-compose.yml')


def exec_cmd(cmd, ignore_errors=False):
    logger.debug('running cmd %s', cmd)
    proc = subprocess.Popen(cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True,
    )
    stdout, stderr = proc.communicate()
    if proc.returncode != 0:
        logger.error('stdout:\n%s', stdout)
        logger.error('stderr:\n%s', stderr)
        if not ignore_errors:
            raise Exception('failed to run %s' % cmd)
    return proc.returncode == 0


class DBHelper:

    def __init__(self, config_broker):
        self.config_broker = config_broker

    def exec_db_command(self, command, db_name='postgres'):
        url = make_url(self.config_broker['DATABASE_URI'])  # We expect this to be a superuser

        if db_name is not None:
            url = url.set(database=db_name)

        db_to_connect = db_name or self.config_broker['DB_NAME']
        env = f"PGPASSWORD={self.config_broker['DB_PASSWORD']}"
        host = f"-h {self.config_broker['DB_HOST']}"
        port = f"-p {self.config_broker['DB_PORT']}"
        db_name_str = f"--dbname {db_to_connect}"
        username_str = f"--username {self.config_broker['DB_USER']}"

        psql_command = (
            f"{env} psql {username_str} {host} {port} {db_name_str} --command \"{command}\""
        )

        return exec_cmd(psql_command)

    def nuke_database(self, db_name):
        assert db_name != 'postgres'

        # Terminate all active connections to the database
        terminate_query = f"""
            SELECT pg_terminate_backend(pid) \
            FROM pg_stat_activity \
            WHERE datname = '{db_name}' AND pid <> pg_backend_pid();
        """
        # self.exec_db_command(terminate_query)

        self.exec_db_command('DROP DATABASE IF EXISTS %s;' % db_name)
        self.exec_db_command('CREATE DATABASE %s;' % db_name)


    def run_db_init_scripts(self):
        mig_scripts_repository = join(dirname(mply_ingester.db.__file__), 'migrations')
        dsn = self.config_broker['DATABASE_URI']

        mh = dbmig.SPMigrationHandler(mig_scripts_repository, dsn, schema='public')
        mh.initialize_db()
        # might need to terminate dangling processes
        mh.migrate(0)
        mh.migrate()

    def reinit_db(self):
        '''
        Initialises or re-initialises a mply_ingester db
        '''
        self.nuke_database('mply_ingester')
        self.run_db_init_scripts()


def start_db_in_docker():
    logger.debug("Starting db in docker container. ")
    cmd = f'docker compose -f {DOCKER_COMPOSE_FILE_PATH} up -d'
    subprocess.check_call(cmd, shell=True)
    # TODO: Wait for the db to be up before continuing


class DBTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config_path = os.environ.get(CONFIG_FILE_ENV_VAR)
        if config_path and not os.path.isfile(config_path):
            raise FileNotFoundError(f"No config file found at: {config_path}")
        config_path = [] if not config_path else [config_path]
        cls.config_broker = ConfigBroker(config_path)
        start_db_in_docker()
        DBHelper(cls.config_broker).reinit_db()
        cls.session = cls.config_broker.get_session()

    @classmethod
    def tearDownClass(cls):
        pass

