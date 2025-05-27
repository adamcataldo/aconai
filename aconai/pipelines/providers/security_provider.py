from datetime import date
from typing import Iterable

import pandas as pd
from aconai.pipelines.data_provider import DataProvider
from aconai.pipelines.data_registry import DataRegistry
import yfinance as yf
from curl_cffi import requests


class SecurityProvider(DataProvider[pd.DataFrame]):
    """
    A class for downloading security price data.
    """

    def __init__(
            self,
            registry: DataRegistry,
            symbol: str,
            start_date: date,
            end_date: date) -> None:
        """
        Initializes the SecurityProvider with the given parameters.
        :param registry: The data registry to use for caching.
        :param symbol: The stock symbol to retrieve data for.
        :param start_date: The start date for the data retrieval.
        :param end_date: The end date for the data retrieva.
        """
        super().__init__(registry)
        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date

    def get_schema(self) -> dict:
        return {
            "type": "record",
            "name": "StockPriceData",
            "namespace": "aconai",
            "fields": [
                {
                "name": "price_data",
                "type": {
                    "type": "array",
                    "items": {
                    "type": "record",
                    "name": "PriceEntry",
                    "fields": [
                        {
                        "name": "date",
                        "type": {
                            "type": "int",
                            "logicalType": "date"
                        }
                        },
                        {
                        "name": "open",
                        "type": "double"
                        },
                        {
                        "name": "high",
                        "type": "double"
                        },
                        {
                        "name": "low",
                        "type": "double"
                        },
                        {
                        "name": "close",
                        "type": "double"
                        },
                        {
                        "name": "adj_open",
                        "type": "double"
                        },
                        {
                        "name": "adj_high",
                        "type": "double"
                        },
                        {
                        "name": "adj_low",
                        "type": "double"
                        },
                        {
                        "name": "adj_close",
                        "type": "double"
                        },
                        {
                        "name": "dividend",
                        "type": "double"
                        },
                        {
                        "name": "split",
                        "type": "double"
                        }
                    ]
                    }
                }
                }
            ]
        }
    
    def get_records(self) -> Iterable[dict]:
        """
        Retrieves the records from yfinance.
        """
        session = requests.Session(impersonate="chrome")
        df = yf.download(
            self.symbol,
            start=self.start_date,
            end=self.end_date,
            session=session,
            auto_adjust=False,
            actions=True
        )
        ratio = (df["Adj Close"] / df["Close"]).to_numpy()
        df["Adj Open"] = df["Open"] * ratio
        df["Adj High"] = df["High"] * ratio
        df["Adj Low"] = df["Low"] * ratio

        price_data = []
        for d, row in df.iterrows():
            entry = {
                "date": d.date(),
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "adj_open": float(row["Adj Open"]),
                "adj_high": float(row["Adj High"]),
                "adj_low": float(row["Adj Low"]),
                "adj_close": float(row["Adj Close"]),
                "dividend": float(row["Dividends"]),
                "split": float(row["Stock Splits"]),
            }
            price_data.append(entry)

        return [{"price_data": price_data}]

    def get_parameters(self) -> dict:
        return {
            "symbol": self.symbol,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat()
        }

    def record_as_type(self, record: dict) -> pd.DataFrame:
        """
        Converts the record to a DataFrame.
        """
        df = pd.DataFrame(record["price_data"])
        return df