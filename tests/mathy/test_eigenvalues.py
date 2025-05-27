import unittest
import numpy as np

from aconai.mathy.eigenvalues import eig

class TestEig(unittest.TestCase):
    def test_eig(self):
        A = np.array([[1.0, 2.0], [3.0, 4.0]])
        lambdas = eig(A)
        expected = np.array([
            (5 + np.sqrt(33)) / 2,
            (5 - np.sqrt(33)) / 2
        ])
        np.testing.assert_almost_equal(lambdas, expected)