def calculate_delta(a: float, b: float) -> float:
    return b - a


def calculate_percents(a: float, b: float) -> float:
    return 100.0 if a == 0 else (b - a) / a * 100


def detect_direction(slope: float, eps: float = 0.0001) -> str:
    """ detect metric direction up or down
    :param slope:
    :param eps:
    :return:
    """
    if slope > eps:
        return "up"
    if slope < -eps:
        return "down"
    return "flat"
