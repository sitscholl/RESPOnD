import logging
from logging import config

config.fileConfig(".config/logging.conf")
logger = logging.getLogger(__name__)