from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

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

    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
    )


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
