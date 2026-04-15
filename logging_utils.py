import logging
import os


def setup_logging(
    service_name: str,
    level: str | None = None,
) -> logging.Logger:
    """Configure unified console logging for all project services."""
    resolved_level = (level or os.getenv("LOG_LEVEL", "INFO")).upper()

    logger = logging.getLogger()
    logger.setLevel(getattr(logging, resolved_level, logging.INFO))

    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    formatter = logging.Formatter(
        fmt=(
            "%(asctime)s | %(levelname)s | %(name)s | "
            "%(process)d:%(threadName)s | %(module)s:%(lineno)d | %(message)s"
        ),
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    logging.getLogger("paramiko").setLevel(logging.WARNING)
    logging.getLogger("pysftp").setLevel(logging.WARNING)
    logging.getLogger("torch").setLevel(logging.WARNING)

    return logging.getLogger(service_name)
