import numpy as np
from numpy.linalg import norm

def qr(A: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    if len(A.shape) != 2:
        raise ValueError("Expected a 2D array as input")
    m, n = A.shape
    if m < n:
        raise ValueError("Matrix has fewer rows than columns")
    Q = np.zeros((m, n))
    R = np.zeros((n, n))
    for k in range(n):
        v = A[:, k]
        for j in range(k):
            R[j, k] = np.dot(Q[:, j], v)
            v = v - R[j, k] * Q[:, j]
        R[k, k] = norm(v)
        Q[:, k] = v / R[k, k]
    return (Q, R)