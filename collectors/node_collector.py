from typing import List, Dict, Any
from config import settings, setup_logging

import psutil
import logging
import requests
import socket
import sys


setup_logging(log_level=settings.log_level, log_file=settings.log_path)
logger = logging.getLogger(__name__)


class Collector:
    def __init__(self, host: str) -> None:
        self.metrics: List[Dict[str, Any]] = []
        self.host = host
        self.vm = socket.gethostname()

    def collect_all(self) -> None:
        """ Collect system metrics
        :return:
        """
        logger.debug("Collect metrics")

        self.collect_load_average_metrics()
        self.collect_load_average_metrics()
        self.collect_load_average_metrics()
        self.collect_disk_metrics()
        self.collect_network_metrics()

    def collect_cpu_metrics(self) -> None:
        self._add("cpu_usage", psutil.cpu_percent(interval=None) / 100)

    def collect_network_metrics(self) -> None:
        net = psutil.net_io_counters()
        self._add("net_bytes_sent", net.bytes_sent)
        self._add("net_bytes_recv", net.bytes_recv)
        self._add("net_packets_sent", net.packets_sent)
        self._add("net_packets_recv", net.packets_recv)


    def collect_load_average_metrics(self) -> None:
        load1, load5, load15 = psutil.getloadavg()
        cpu_count = psutil.cpu_count()

        self._add("load_1_norm", load1 / cpu_count)
        self._add("load_5_norm", load5 / cpu_count)
        self._add("load_15_norm", load15 / cpu_count)

    def collect_ram_metrics(self) -> None:
        mem = psutil.virtual_memory()
        self._add("ram_used_pct", mem.percent / 100)

    def collect_disk_metrics(self) -> None:
        disk = psutil.disk_usage("/")
        self._add("disk_used_pct", disk.percent / 100, {"mount": "/"})

    def _add(self, metric: str, value: float, tags: Dict[str, str] | None = None) -> None:
        self.metrics.append({
            "host": self.host,
            "vm": self.vm,
            "metric": metric,
            "value": value,
            "tags": tags or {}
        })


def send(metrics: List[Dict[str, Any]]):
    """ Send metrics to analytics api
    :param metrics:
    :return:
    """
    logger.info("Send metrics")
    try:
        requests.post(settings.api_url, json=metrics, timeout=3)
    except Exception as e:
        logger.error(f"Send metrics failed: {e}")


def main():
    collector = Collector(host=settings.host)
    try:
        collector.collect_all()
        if not collector.metrics:
            logger.warning("No metrics collected")
            return

        logger.info(f"Send {len(collector.metrics)} metrics")
        send(metrics=collector.metrics)

        logger.info("Collector finished successfully")
    except Exception as e:
        logger.exception(f"Collector failed - {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
