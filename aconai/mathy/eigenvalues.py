import numpy as np
from numpy.linalg import qr

def _max_sub_diag(A: np.ndarray) -> float:
    m = 0.0
    n = A.shape[0]
    for i in range(n):
        for j in range(n):
            if i > j and abs(A[i, j]) > m:
                m = abs(A[i, j])
    return m

def eig(A: np.ndarray, tol: float = 1e-6) -> np.ndarray:
    if len(A.shape) != 2 or A.shape[0] != A.shape[1]:
        raise ValueError("A must be a square matrix")
    while _max_sub_diag(A) > tol:
        res = qr(A)
        Q = res.Q
        R  = res.R
        A = R @ Q
    return np.diagonal(A)