from typing import Tuple, TypeAlias
from pandas import DataFrame
from torch import Tensor
import torch
from torch.utils.data import Dataset
import numpy as np

Columns: TypeAlias = str | list[str]

class RowAccessor(Dataset[Tuple[Tensor, Tensor]]):
    """
    A class to access rows of a DataFrame as PyTorch tensors.
    This class is used to create a dataset from a DataFrame, where each row
    is converted to a tuple of tensors.
    """
    def __init__(self,  
                 df: DataFrame,
                 labels: Columns):
        """
        Args:
            df (DataFrame): DataFrame containing the data.
            labels (Columns): Column names for the labels.
        """
        label_cols = [labels] if isinstance(labels, str) else labels
        missing = [c for c in label_cols if c not in df.columns]
        if missing:
            raise ValueError(
                f"Label column(s) {missing} not present in DataFrame columns" 
                f" {list(df.columns)}"
            )
        feature_df = df.drop(columns=label_cols)
        label_df = df[label_cols]
        if feature_df.empty:
            raise ValueError("After dropping label columns, no feature columns"
                             " remain.")
        self._features = torch.as_tensor(
            feature_df.to_numpy(dtype=np.float32, copy=True),  # force one numeric dtype
            dtype=torch.float32,
        )
        self._labels = torch.as_tensor(
            label_df.to_numpy(dtype=np.float32, copy=True),    # or int64 if preferred
            dtype=torch.float32,
        )
        self.feature_names = feature_df.columns
        self.label_names = label_cols

    def __len__(self) -> int:
        return self._features.shape[0]

    def __getitem__(self, idx: int) -> Tuple[Tensor, Tensor]:
        return self._features[idx], self._labels[idx]