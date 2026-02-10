from collector import Collector
from sender import Sender
from config import settings, setup_logging

import logging
import sys

setup_logging(log_level=settings.log_level, log_file=settings.log_path)
logger = logging.getLogger(__name__)


def main():
    collector = Collector(
        host=settings.host,
        allowed_metrics=settings.allowed_metrics
    )
    sender = Sender(
        api_url=settings.api_url
    )

    try:
        collector.collect_all(enabled=set(settings.enabled_list))
        if not collector.metrics:
            logger.warning("No metrics collected")
            return

        logger.info(f"Send {len(collector.metrics)} metrics")
        sender.send(metrics=collector.metrics)

        logger.info("Collector finished successfully")
    except Exception as e:
        logger.exception(f"Collector failed - {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
