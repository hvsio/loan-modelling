import logging
import sys
import os

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())


def get_logger():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logger = logging.getLogger()
    return logger


def get_db_url():
    return (
        f"postgresql://{os.environ.get('db_user')}"
        f":{os.environ.get('db_pass')}"
        f"@{os.environ.get('db_host')}"
        f":{str(os.environ.get('db_port'))}"
        f"/{os.environ.get('db_name')}"
    )
