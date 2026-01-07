from typing import List, Dict, Any
from config import settings, setup_logging

import psutil
import logging
import requests
import socket


setup_logging(log_level=settings.log_level, log_file=settings.log_path)
logger = logging.getLogger(__name__)

VM = socket.gethostname()


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

    load1, load5, load15 = psutil.getloadavg()
    metrics.extend([
        {"host": settings.host, "vm": VM, "metric": "load_1", "value": load1, "tags": {}},
        {"host": settings.host, "vm": VM, "metric": "load_5", "value": load5, "tags": {}},
        {"host": settings.host, "vm": VM, "metric": "load_15", "value": load15, "tags": {}},
    ])

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


if __name__ == "__main__":
    send(metrics=collect_metrics())
