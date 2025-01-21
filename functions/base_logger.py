import logging
from logging import config

config.fileConfig("logging.conf")
logger = logging.getLogger(__name__)