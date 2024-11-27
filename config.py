import logging

from loguru import logger



class InterceptHandler(logging.Handler):  # noqa: F811
    def emit(self, record):
        logger_opt = logger.opt(depth=6, exception=record.exc_info)
        logger_opt.log(record.levelno, record.getMessage())


class Formatter:
    def __init__(self):
        self.padding = 0
        self.fmt = "{time} | {level: <8} | {name}:{function}:{line}{extra[padding]} | {message}\n{exception}"

    def format(self, record):
        length = len("{name}:{function}:{line}".format(**record))
        self.padding = max(self.padding, length)
        record["extra"]["padding"] = " " * (self.padding - length)
        return self.fmt


formatter = Formatter()
logger = logger
# logger.add(sys.stderr, format=formatter)
logger.add("debug.log", level="DEBUG", rotation="1 day",
           compression="zip", backtrace=True, retention=7)
# logging.basicConfig(handlers=[InterceptHandler()], level=0)
logger.opt(exception=True)
