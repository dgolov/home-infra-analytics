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
        cpu_times = psutil.cpu_times_percent()

        self._add("cpu_usage", psutil.cpu_percent(interval=None) / 100)
        self._add("cpu_user", cpu_times.user / 100)
        self._add("cpu_system", cpu_times.system / 100)
        self._add("cpu_iowait", getattr(cpu_times, "iowait", 0.0) / 100)

    def collect_network_metrics(self) -> None:
        net = psutil.net_io_counters()

        self._add("net_bytes_sent", net.bytes_sent)
        self._add("net_bytes_recv", net.bytes_recv)
        self._add("net_packets_sent", net.packets_sent)
        self._add("net_packets_recv", net.packets_recv)
        self._add("net_err_in", net.errin)
        self._add("net_err_out", net.errout)
        self._add("net_drop_in", net.dropin)
        self._add("net_drop_out", net.dropout)


    def collect_load_average_metrics(self) -> None:
        load1, load5, load15 = psutil.getloadavg()
        cpu_count = psutil.cpu_count()

        self._add("load_1_norm", load1 / cpu_count)
        self._add("load_5_norm", load5 / cpu_count)
        self._add("load_15_norm", load15 / cpu_count)

    def collect_ram_metrics(self) -> None:
        mem = psutil.virtual_memory()
        swap = psutil.swap_memory()

        self._add("ram_used_pct", mem.percent / 100)
        self._add("ram_available_bytes", mem.available)
        self._add("swap_used_pct", swap.percent / 100)

    def collect_disk_metrics(self) -> None:
        disk = psutil.disk_usage("/")
        io = psutil.disk_io_counters()

        self._add("disk_used_pct", disk.percent / 100, {"mount": "/"})
        self._add("disk_read_bytes", io.read_bytes)
        self._add("disk_write_bytes", io.write_bytes)
        self._add("disk_read_time_ms", io.read_time)
        self._add("disk_write_time_ms", io.write_time)

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
