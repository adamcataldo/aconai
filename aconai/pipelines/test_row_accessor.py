import unittest
import pandas as pd
import torch
from aconai.pipelines.row_accessor import RowAccessor

class TestRowAccessor(unittest.TestCase):
    def test_row_accessor_getitem(self):
        data = {
            "feature1": [1.0, 2.0, 3.0],
            "feature2": [4.0, 5.0, 6.0],
            "label": [0, 1, 0],
        }
        df = pd.DataFrame(data)
        dataset = RowAccessor(df, labels="label")

        features, labels = dataset[1]
        self.assertIsInstance(features, torch.Tensor)
        self.assertIsInstance(labels, torch.Tensor)
        self.assertTrue(torch.equal(features, torch.tensor([2.0, 5.0])))
        self.assertTrue(torch.equal(labels, torch.tensor([1])))

if __name__ == "__main__":
    unittest.main()