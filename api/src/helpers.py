def calculate_delta(a: float, b: float) -> float:
    return b - a


def calculate_percents(a: float, b: float) -> float:
    return 100.0 if a == 0 else (b - a) / a * 100
