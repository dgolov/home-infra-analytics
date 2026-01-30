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


def collect_network_metrics(metrics: List[Dict[str, Any]]) -> None:
    net = psutil.net_io_counters()
    metrics.extend([
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


def collect_load_average_metrics(metrics: List[Dict[str, Any]]) -> None:
    load1, load5, load15 = psutil.getloadavg()
    cpu_count = psutil.cpu_count()

    metrics.extend([
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


def collect_metrics() -> List[Dict[str, Any]]:
    """ Collect system metrics
    :return:
    """
    logger.debug("Collect metrics")
    metrics: List[Dict[str, Any]] = [{
        "host": settings.host,
        "vm": VM,
        "metric": "cpu_usage",
        "value": psutil.cpu_percent(interval=None) / 100,
        "tags": {}
    }]

    collect_load_average_metrics(metrics=metrics)

    mem = psutil.virtual_memory()
    metrics.append({
        "host": settings.host,
        "vm": VM,
        "metric": "ram_used_pct",
        "value": mem.percent / 100,
        "tags": {}
    })

    disk = psutil.disk_usage("/")
    metrics.append({
        "host": settings.host,
        "vm": VM,
        "metric": "disk_used_pct",
        "value": disk.percent / 100,
        "tags": {"mount": "/"}
    })

    collect_network_metrics(metrics=metrics)

    return metrics


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
    try:
        metrics = collect_metrics()
        if not metrics:
            logger.warning("No metrics collected")
            return

        logger.info(f"Send {len(metrics)} metrics")
        send(metrics=metrics)

        logger.info("Collector finished successfully")
    except Exception as e:
        logger.exception(f"Collector failed - {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
