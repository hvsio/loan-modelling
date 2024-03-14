import logging
import os

from dotenv import find_dotenv, load_dotenv
from sqlalchemy import inspect
from sqlalchemy.engine.base import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.schema import CreateSchema
from utils import get_db_connection
from db_models import create_tables

logger = logging.getLogger('postgres_logger')
load_dotenv(find_dotenv())

def create_db(engine: Engine, conn: Connection, type="train"):
    schemaname = os.environ.get(f'schema_name_{type}')
    try:
        if not schemaname:
                logger.error(f"Missing {type} schema name.")

        if not inspect(conn).has_schema(schema_name=schemaname):
                conn.execute(CreateSchema(name=schemaname))
                conn.commit()
        
        metadata = create_tables(schemaname, True if type == "train" else False)
        metadata.create_all(engine)
    except SQLAlchemyError as e:
        logger.error(f'Error establishing the DB: {e}')
        conn.rollback()

with get_db_connection(True) as (engine, conn):
        logger.info(f'Established connection with db engine: {engine}')

        create_db(engine, conn)
        create_db(engine, conn, "test")

        # add autoincrement to ids
        # conn.execute(
        #     text(
        #         """
        #             ALTER TABLE metals_analytics.metal_prices ALTER COLUMN id SET DEFAULT nextval('metals_sequence');
        #             """
        #     )
        # )

        conn.commit()
    
