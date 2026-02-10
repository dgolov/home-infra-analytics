from typing import TypedDict, Dict

class Metric(TypedDict):
    host: str
    vm: str
    metric: str
    value: float
    tags: Dict[str, str]
