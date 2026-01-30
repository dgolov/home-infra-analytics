from typing import List, Dict, Any
from config import settings, setup_logging

import psutil
import logging
import requests
import socket
import sys


setup_logging(log_level=settings.log_level, log_file=settings.log_path)
logger = logging.getLogger(__name__)

VM = socket.gethostname()


class Collector:
    def __init__(self) -> None:
        self.metrics: List[Dict[str, Any]] = []

    def collect_cpu_metrics(self) -> None:
        self.metrics.append(
            {
                "host": settings.host,
                "vm": VM,
                "metric": "cpu_usage",
                "value": psutil.cpu_percent(interval=None) / 100,
                "tags": {}
            }
        )

    def collect_network_metrics(self) -> None:
        net = psutil.net_io_counters()
        self.metrics.extend([
            {
                "host": settings.host,
                "vm": VM,
                "metric": "net_bytes_sent",
                "value": net.bytes_sent,
                "tags": {}
            },
            {
                "host": settings.host,
                "vm": VM,
                "metric": "net_bytes_recv",
                "value": net.bytes_recv,
                "tags": {}
            },
            {
                "host": settings.host,
                "vm": VM,
                "metric": "net_packets_sent",
                "value": net.packets_sent,
                "tags": {}
            },
            {
                "host": settings.host,
                "vm": VM,
                "metric": "net_packets_recv",
                "value": net.packets_recv,
                "tags": {}
            },
        ])


    def collect_load_average_metrics(self) -> None:
        load1, load5, load15 = psutil.getloadavg()
        cpu_count = psutil.cpu_count()

        self.metrics.extend([
            {
                "host": settings.host,
                "vm": VM,
                "metric": "load_1_norm",
                "value": load1 / cpu_count,
                "tags": {}
            },
            {
                "host": settings.host,
                "vm": VM,
                "metric": "load_5_norm",
                "value": load5 / cpu_count,
                "tags": {}
            },
            {
                "host": settings.host,
                "vm": VM,
                "metric": "load_15_norm",
                "value": load15 / cpu_count,
                "tags": {}
            },
        ])

    def collect_ram_metrics(self) -> None:
        mem = psutil.virtual_memory()
        self.metrics.append({
            "host": settings.host,
            "vm": VM,
            "metric": "ram_used_pct",
            "value": mem.percent / 100,
            "tags": {}
        })

    def collect_disk_metrics(self) -> None:
        disk = psutil.disk_usage("/")
        self.metrics.append({
            "host": settings.host,
            "vm": VM,
            "metric": "disk_used_pct",
            "value": disk.percent / 100,
            "tags": {"mount": "/"}
        })

    def collect(self) -> None:
        """ Collect system metrics
        :return:
        """
        logger.debug("Collect metrics")

        self.collect_load_average_metrics()
        self.collect_load_average_metrics()
        self.collect_load_average_metrics()
        self.collect_disk_metrics()
        self.collect_network_metrics()


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
    collector = Collector()
    try:
        collector.collect()
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
