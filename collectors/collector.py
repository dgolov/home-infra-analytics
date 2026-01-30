from typing import List, Dict, Any

import logging
import psutil
import socket


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
        """ Collect CPU metrics
        """
        cpu_times = psutil.cpu_times_percent()

        self._add("cpu_usage", psutil.cpu_percent(interval=None) / 100)
        self._add("cpu_user", cpu_times.user / 100)
        self._add("cpu_system", cpu_times.system / 100)
        self._add("cpu_iowait", getattr(cpu_times, "iowait", 0.0) / 100)

    def collect_network_metrics(self) -> None:
        """ Collect network metrics
        """
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
        """ Collect load average metrics
        """
        load1, load5, load15 = psutil.getloadavg()
        cpu_count = psutil.cpu_count()

        self._add("load_1_norm", load1 / cpu_count)
        self._add("load_5_norm", load5 / cpu_count)
        self._add("load_15_norm", load15 / cpu_count)

    def collect_ram_metrics(self) -> None:
        """ Collect RAM metrics
        """
        mem = psutil.virtual_memory()
        swap = psutil.swap_memory()

        self._add("ram_used_pct", mem.percent / 100)
        self._add("ram_available_bytes", mem.available)
        self._add("swap_used_pct", swap.percent / 100)

    def collect_disk_metrics(self) -> None:
        """ Collect disk metrics
        """
        disk = psutil.disk_usage("/")
        io = psutil.disk_io_counters()

        self._add("disk_used_pct", disk.percent / 100, {"mount": "/"})
        self._add("disk_read_bytes", io.read_bytes)
        self._add("disk_write_bytes", io.write_bytes)
        self._add("disk_read_time_ms", io.read_time)
        self._add("disk_write_time_ms", io.write_time)

    def _add(self, metric: str, value: float, tags: Dict[str, str] | None = None) -> None:
        """ Add a metric
        """
        self.metrics.append({
            "host": self.host,
            "vm": self.vm,
            "metric": metric,
            "value": value,
            "tags": tags or {}
        })
