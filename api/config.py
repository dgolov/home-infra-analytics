from pydantic_settings import BaseSettings, SettingsConfigDict

import logging
import sys


LOG_FORMAT = "[%(asctime)s] [%(levelname)s] %(name)s: %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class Settings(BaseSettings):
    log_level: str = "DEBUG"
    log_path: str = "app.log"

    app_name: str = "InfraAnalyticsAPI"
    debug: bool = False

    clickhouse_host: str
    clickhouse_port: int
    clickhouse_db: str
    clickhouse_user: str
    clickhouse_password: str

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )

    @property
    def clickhouse_url(self) -> str:
        return f"http://{self.clickhouse_host}:{self.clickhouse_port}"


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


settings = Settings()
