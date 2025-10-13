import numpy as np


def correlation(exact, estimate) -> float:
    return np.corrcoef(exact, estimate)[0, 1]


def l1_norm(exact, estimate) -> float:
    return np.sum(np.abs(exact - estimate))


def l2_norm(exact, estimate) -> float:
    return np.sum(np.square(exact - estimate))
