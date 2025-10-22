# quant_alpha_model/data_loader.py
import os
import pandas as pd
import yfinance as yf
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime


def fetch_price_data(tickers, start="2015-01-01", end=None, save=True):
    """
    Download adjusted close prices for given tickers using yfinance.
    Saves both raw and cleaned data.
    """
    if end is None:
        end = datetime.today().strftime("%Y-%m-%d")

    print(
        f"Downloading data for {len(tickers)} tickers from {start} to {end}...")

    # Extract Close or Adj Close per ticker
    data_dict = {}
    for t in tickers:
        try:
            df = yf.download(t, start=start, progress=False)
            if "Adj Close" in df[t].columns:
                data_dict[t] = df[t]["Adj Close"]
            else:
                data_dict[t] = df[t]["Close"]
        except Exception as e:
            print(f"⚠️ Skipping {t}: {e}")

    data = pd.DataFrame(data_dict)

    # Save raw version
    if save:
        os.makedirs("data/raw", exist_ok=True)
        raw_path = os.path.join(
            'data/raw', f"prices_raw_{datetime.today().strftime('%Y%m%d')}.parquet"
        )
        table = pa.Table.from_pandas(data)
        pq.write_table(table, raw_path)
        print(f"✅ Saved raw data to {raw_path}")

    return data
