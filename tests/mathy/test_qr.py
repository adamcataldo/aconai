import unittest
import numpy as np

from aconai.mathy.qr import qr

class TestQR(unittest.TestCase):
    def test_qr(self):
        A = np.array([[4.0, 1.0], [1.0, 3.0]])
        Q, R = qr(A)
        maybe_I = np.transpose(Q) @ Q
        np.testing.assert_almost_equal(maybe_I, np.identity(2))
        np.testing.assert_almost_equal(R, np.triu(R))
        np.testing.assert_almost_equal(Q @ R, A)