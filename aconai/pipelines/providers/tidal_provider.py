import datetime as dt
from dateutil.relativedelta import relativedelta
from typing import Iterator, List
import numpy as np
import pandas as pd
import requests
from aconai.pipelines.data_provider import DataProvider
from aconai.pipelines.data_registry import DataRegistry


class TidalProvider(DataProvider[pd.DataFrame]):
    """
    A class for getting high/low tidal data.
    """

    def __init__(
            self,
            registry: DataRegistry,
            station: int,
            start_date: dt.date,
            years: int) -> None:
        """
        Initializes the TidalProvider with the given parameters.
        :param registry: The data registry to use for caching.
        :param station: The tidal station ID to retrieve data for.
        :param start_date: The start date for the data retrieval.
        :param years: The number of years of data to retrieve.
        """ 
        super().__init__(registry)
        self.station = station
        self.start_date = start_date
        self.years = years

    def get_parameters(self) -> dict:
        """
        Returns the parameters for the data provider.
        """
        return {
            "station": self.station,
            "start_date": self.start_date.isoformat(),
            "years": self.years
        }

    def get_schema(self) -> dict:
        return {
            "namespace": "gov.noaa.coops.tides",
            "type": "record",
            "name": "TideYearData",
            "doc": "Verified high/low‑water observations returned by the NOAA CO‑OPS high_low product for a single station and one‑year window.",
            "fields": [
                {
                "name": "extremes",
                "type": {
                    "type": "array",
                    "items": {
                    "name": "TideExtreme",
                    "type": "record",
                    "fields": [
                        {
                        "name": "timestamp",
                        "type": { "type": "long", "logicalType": "timestamp-millis" },
                        "doc": "Local time of the high/low (epoch‑millis)."
                        },
                        {
                        "name": "extreme_type",
                        "type": {
                            "type": "enum",
                            "name": "ExtremeType",
                            "symbols": [ "HH", "H", "L", "LL" ]
                        },
                        "doc": "HH = Higher‑High, H = High, L = Low, LL = Lower‑Low."
                        },
                        {
                        "name": "height",
                        "type": "double",
                        "doc": "Water level relative to the chosen datum."
                        }
                    ]
                    }
                },
                "doc": "Ordered list of successive high/low‑water observations."
                }
            ]
        }
    
    def _get_one_year(self, start_date) -> pd.DataFrame:
        """
        Returns one year's worth of data.
        """
        station = self.station
        datum = "MLLW"
        units = "metric"
        time_zone = "lst_ldt"
        application = "aconai"
        if isinstance(start_date, str):
            start_date = dt.datetime.strptime(
                start_date.replace("-", ""), "%Y%m%d"
            ).date()
        elif isinstance(start_date, dt.datetime):
            start_date = start_date.date()

        end_date = start_date + dt.timedelta(days=365) - dt.timedelta(seconds=1)
        fmt = "%Y%m%d"

        params = {
            "product": "high_low",
            "begin_date": start_date.strftime(fmt),
            "end_date": end_date.strftime(fmt),
            "datum": datum,
            "station": station,
            "units": units,
            "time_zone": time_zone,
            "format": "json",
            "application": application,
        }

        url = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        raw = resp.json()

        df = pd.DataFrame(raw["data"])
        df = df.rename(columns={"t": "timestamp",
                                "v": "height"})
        if "f" in df.columns:
            df = df.drop(columns=["f"])

        if "ty" in df.columns:
            df["type"] = df["ty"].str.strip()
            df = df.drop(columns=["ty"])
        else:
            raise KeyError("Expected column 'ty' not found in API response")
        
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["height"]    = pd.to_numeric(df["height"], errors="coerce")
        df = df[["timestamp", "type", "height"]]

        return df

    def _get_multi_years(self) -> pd.DataFrame:
        """
        Fetch `years` consecutive years of verified high/low‑water data
        starting at *start_date* (inclusive) and return a single merged
        DataFrame sorted by timestamp.
        """
        start_date = self.start_date
        years = self.years
        if isinstance(start_date, str):
            current_start = dt.datetime.strptime(
                start_date.replace("-", ""), "%Y%m%d"
            ).date()
        elif isinstance(start_date, dt.datetime):
            current_start = start_date.date()
        else:  # already a date
            current_start = start_date

        yearly_frames: List[pd.DataFrame] = []

        for _ in range(years):
            yearly_frames.append(self._get_one_year(current_start))
            current_start = current_start + relativedelta(years=1)

        full_df = (
            pd.concat(yearly_frames, ignore_index=True)
              .drop_duplicates(subset=["timestamp", "type"])
              .sort_values("timestamp")
              .reset_index(drop=True)
        )
        return full_df
    
    def _get_avro_object(
        self,
    ) -> dict:
        """
        Fetch *years* years of tide data beginning at *start_date* and return
        a dict that exactly matches the TideYearData Avro schema:
        """
        df = self._get_multi_years()

        UTC = dt.timezone.utc        
        ts_aware = (
            df["timestamp"]
              .dt.round("ms")
              .apply(
                  lambda ts: ts.to_pydatetime().replace(tzinfo=UTC)
              )
        )
        extremes = pd.DataFrame({
            "timestamp":    ts_aware,
            "extreme_type": df["type"].astype(str),
            "height":       df["height"].astype("float64"),
        }).to_dict(orient="records")
        return {"extremes": extremes}    

    def get_records(self) -> Iterator[dict]:
        """
        Retrieves the records from NOAA.
        """
        yield self._get_avro_object()

    def record_as_type(self, record: dict) -> pd.DataFrame:
        """
        Converts the record to a DataFrame.
        """
        extremes = record.get("extremes", [])
        df = pd.DataFrame(extremes)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        df = (
            df.rename(columns={"extreme_type": "type"})
              .astype({"height": "float64"})
              .sort_values("timestamp")
              .reset_index(drop=True)
              [ ["timestamp", "type", "height"] ]
        )
        return df