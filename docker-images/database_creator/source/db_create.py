import logging
import os
import sys

from db_models import create_tables
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import inspect
from sqlalchemy.engine.base import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.schema import CreateSchema
from utils import get_db_connection

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger()
handler = logging.StreamHandler()
logger.addHandler(handler)
load_dotenv(find_dotenv())


def create_db(engine: Engine, conn: Connection, type: str):
    schemaname = os.environ.get(f'schema_name_{type}')
    try:
        if not schemaname:
            logger.error(f'Missing {type} schema name.')
            exit(1)

        logger.info(f'Schema of choice: {schemaname}')
        if not inspect(conn).has_schema(schema_name=schemaname):
            logger.info('Could not find required schema, creating one...')
            conn.execute(CreateSchema(name=schemaname))
            conn.commit()

        logger.info("Creating schema's tables...")
        metadata = create_tables(schemaname)
        metadata.create_all(engine)

    except SQLAlchemyError as e:
        logger.error(f'Error establishing the DB: {e}')
        conn.rollback()
        exit(1)


with get_db_connection(True) as (engine, conn):
    try:
        logger.info(f'Established connection with db engine: {engine}')

        logger.info('Creating train schema...')
        create_db(engine, conn, 'train')
        logger.info('Creating test schema...')
        create_db(engine, conn, 'test')

        conn.commit()
        logger.info('Success.')
    except SQLAlchemyError as e:
        logger.error(f'Error establishing databses: {e}')
        conn.rollback()
        exit(1)
