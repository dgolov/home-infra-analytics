from models import Metric
from typing import Callable, List, Dict, Optional, Set

import logging
import psutil
import socket


logger = logging.getLogger(__name__)


class Collector:
    def __init__(self, host: str, allowed_metrics: List[str]) -> None:
        self.metrics: List[Metric] = []
        self.host: str = host
        self.vm: str = socket.gethostname()
        self.allowed_metrics: Set[str] = set(allowed_metrics)
        self.collectors: Dict[str, Callable[[], None]] = {
            "cpu": self.collect_cpu_metrics,
            "ram": self.collect_ram_metrics,
            "disk": self.collect_disk_metrics,
            "net": self.collect_network_metrics,
            "load": self.collect_load_average_metrics
        }

    def collect_all(self, enabled: Set[str]) -> None:
        """ Collect system metrics
        :return:
        """
        if not enabled:
            enabled = self.collectors.keys()

        logger.debug(f"Collect metrics. Enabled: {enabled}, allowed: {self.allowed_metrics}")

        self.metrics.clear()

        for name in enabled:
            try:
                collector: Optional[Callable[[], None]] = self.collectors.get(name)
                if not collector:
                    logger.warning(f"Unknown collector: {name}")
                    continue
                collector()

            except Exception as e:
                logger.error(f"Failed to collect {name} metrics - {e}")

    def collect_cpu_metrics(self) -> None:
        """ Collect CPU metrics
        """
        logger.info(f"Collect CPU metrics")

        cpu_percent = psutil.cpu_percent(interval=None)
        cpu_times = psutil.cpu_times_percent()

        self._add("cpu_usage", self._pct(cpu_percent))
        self._add("cpu_user", self._pct(cpu_times.user))
        self._add("cpu_system", self._pct(cpu_times.system))
        self._add("cpu_iowait", getattr(cpu_times, "iowait", 0.0) / 100)

    def collect_network_metrics(self) -> None:
        """ Collect network metrics
        """
        logger.info(f"Collect network metrics")

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
        logger.info(f"Collect load average metrics")

        try:
            load1, load5, load15 = psutil.getloadavg()
        except OSError:
            return
        cpu_count = psutil.cpu_count()

        self._add("load_1_norm", load1 / cpu_count)
        self._add("load_5_norm", load5 / cpu_count)
        self._add("load_15_norm", load15 / cpu_count)

    def collect_ram_metrics(self) -> None:
        """ Collect RAM metrics
        """
        logger.info(f"Collect RAM metrics")

        mem = psutil.virtual_memory()
        swap = psutil.swap_memory()

        self._add("ram_used_pct", self._pct(mem.percent))
        self._add("ram_available_bytes", mem.available)
        self._add("swap_used_pct", self._pct(swap.percent))

    def collect_disk_metrics(self) -> None:
        """ Collect disk metrics
        """
        logger.info(f"Collect disk metrics")

        disk = psutil.disk_usage("/")
        io = psutil.disk_io_counters()

        self._add("disk_used_pct", self._pct(disk.percent), {"mount": "/"})
        self._add("disk_read_bytes", io.read_bytes)
        self._add("disk_write_bytes", io.write_bytes)
        self._add("disk_read_time_ms", io.read_time)
        self._add("disk_write_time_ms", io.write_time)

    @ staticmethod
    def _pct(value: float) -> float:
        return value / 100.0

    def _add(self, metric: str, value: float, tags: Dict[str, str] | None = None) -> None:
        """ Add a metric
        """
        if metric not in self.allowed_metrics:
            logger.debug(f"Metric {metric} skipped")
            return

        logger.debug(f"Metric {metric} added, value: {value}, tags: {tags}")

        m: Metric = {
            "host": self.host,
            "vm": self.vm,
            "metric": metric,
            "value": float(value),
            "tags": tags or {},
        }
        self.metrics.append(m)
