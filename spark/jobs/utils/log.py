import logging
from skyhealth.config import settings


def configure():
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    logging.basicConfig(level=level, format='%(asctime)s %(levelname)s %(message)s')
