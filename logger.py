import logging

from logging.handlers import TimedRotatingFileHandler

import os

os.makedirs("logs",exist_ok=True)

logger=logging.getLogger("translator")

logger.setLevel(logging.INFO)

formatter=logging.Formatter(
    "%(asctime)s %(levelname)s %(message)s"
)

handler=TimedRotatingFileHandler(
    "logs/service.log",
    when="midnight",
    backupCount=30,
    encoding="utf8"
)

handler.setFormatter(formatter)

logger.addHandler(handler)

console=logging.StreamHandler()

console.setFormatter(formatter)

logger.addHandler(console)