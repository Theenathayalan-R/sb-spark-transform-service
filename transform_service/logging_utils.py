import logging
import sys
from typing import Any, Optional, Type

try:
    from pythonjsonlogger.json import JsonFormatter
    JsonFormatterType: Optional[Type[Any]] = JsonFormatter
except ImportError:  # pragma: no cover
    JsonFormatterType = None


def setup_logging(log_level: str = "INFO"):
    """Sets up structured JSON logging."""
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Remove any existing handlers to avoid duplicate logs
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create a handler to write to stdout
    log_handler = logging.StreamHandler(sys.stdout)

    # Create a JSON formatter when available, else basic formatter
    if JsonFormatterType is not None:
        formatter = JsonFormatterType(
            "%(asctime)s %(name)s %(levelname)s %(message)s %(job_id)s"
        )
    else:  # pragma: no cover
        formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")

    log_handler.setFormatter(formatter)

    root_logger.addHandler(log_handler)


class JobIdLogger(logging.LoggerAdapter):
    """A logger adapter to add job_id to log records."""

    def process(self, msg, kwargs):
        if "extra" not in kwargs:
            kwargs["extra"] = {}
        # Ensure job_id is always present in the log record's extra dictionary
        if self.extra:
            kwargs["extra"]["job_id"] = self.extra.get("job_id", "N/A")
        else:
            kwargs["extra"]["job_id"] = "N/A"
        return msg, kwargs
