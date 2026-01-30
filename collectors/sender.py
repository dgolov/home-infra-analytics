from typing import List, Dict, Any

import logging
import requests


logger = logging.getLogger(__name__)


class Sender:
    def __init__(self, api_url: str, timeout: int = 3):
        self.api_url = api_url
        self.timeout = timeout

    def send(self, metrics: List[Dict[str, Any]]):
        """ Send metrics to analytics api
        :param metrics:
        :return:
        """
        logger.info("Send metrics")
        try:
            requests.post(
                self.api_url,
                json=metrics,
                timeout=self.timeout
            )
        except Exception as e:
            logger.error(f"Send metrics failed: {e}")
