import logging
from logging import Logger

import sqlparse


def set_context(logger, value):
    """
    Walks the tree of loggers and tries to set the context for each handler

    :param logger: logger
    :param value: value to set
    """
    _logger = logger
    while _logger:
        for handler in _logger.handlers:
            try:
                handler.set_context(value)
            except AttributeError:
                # Not all handlers need to have context passed in so we ignore
                # the error when handlers do not have set_context defined.
                pass
        if _logger.propagate is True:
            _logger = _logger.parent
        else:
            _logger = None


class LoggingMixin:
    """Convenience super-class to have a logger configured with the class name"""

    def __init__(self, context=None):
        self._set_context(context)

    @property
    def log(self) -> Logger:
        """Returns a logger."""
        try:
            # FIXME: LoggingMixin should have a default _log field.
            return self._log
        except AttributeError:
            self._log = logging.getLogger(self.__class__.__module__ + '.' + self.__class__.__name__)
            return self._log

    def _set_context(self, context):
        if context is not None:
            set_context(self.log, context)

    def log_sql(self, query: str):
        # all_statements = sqlparse.split(query)

        # for statement in all_statements:
        formatted_query = sqlparse.format(
            sql=query,
            # reindent=True,
            reindent_aligned=True,
            keyword_case='upper',
            identifier_case='upper',
            # strip_whitespace=True
        )
        print('\n' + formatted_query)
        # self.log.info(formatted_query)
