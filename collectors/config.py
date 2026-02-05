from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Optional

import logging
import sys


LOG_FORMAT = "[%(asctime)s] [%(levelname)s] %(name)s: %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
BASE_DIR = Path(__file__).resolve().parent


class Settings(BaseSettings):
    log_level: str = "DEBUG"
    log_path: str = "app.log"

    api_url: str
    host: str

    enabled: Optional[str] = None
    metrics: str = ""

    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
    )

    @property
    def enabled_list(self) -> List[str]:
        if self.enabled:
            return self.enabled.split(",")
        return []

    @property
    def allowed_metrics(self) -> List[str]:
        if self.metrics:
            return self.metrics.split(",")

        return [
            "cpu_usage",
            "cpu_user",
            "cpu_system",
            "cpu_iowait",
            "net_bytes_sent",
            "net_bytes_recv",
            "net_packets_sent",
            "net_packets_recv",
            "net_err_in",
            "net_err_out",
            "net_drop_in",
            "net_drop_out",
            "load_1_norm",
            "load_5_norm",
            "load_15_norm",
            "ram_used_pct",
            "ram_available_bytes",
            "swap_used_pct",
            "disk_used_pct",
            "disk_read_bytes",
            "disk_write_bytes",
            "disk_read_time_ms",
            "disk_write_time_ms",
        ]


def setup_logging(log_level: str = "DEBUG", log_file: str = "app.log") -> None:
    """ Настройки логирования
    :param log_level:
    :param log_file:
    :return:
    """
    level = getattr(logging, log_level.upper(), logging.DEBUG)

    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file)
    ]

    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT,
        handlers=handlers
    )

    logging.getLogger("urllib3").setLevel(logging.WARNING)


settings = Settings()
