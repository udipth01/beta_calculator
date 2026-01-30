import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta

END_DATE = datetime.utcnow().date()
START_DATE = END_DATE - timedelta(days=365)
INDEX_TICKER = "^NSEI"


def download_yahoo_adjclose(ticker, start, end):
    data = yf.download(
        ticker,
        start=start.isoformat(),
        end=(end + timedelta(days=1)).isoformat(),
        auto_adjust=True,
        progress=False,
        threads=False
    )
    if data is None or data.empty:
        return None
    return data["Close"]


def compute_beta(stock_series, index_series):
    df = pd.concat([stock_series, index_series], axis=1, join="inner").dropna()
    df = df.sort_index()

    if len(df) < 30:
        return np.nan

    returns = df.pct_change().dropna()
    if len(returns) < 20:
        return np.nan

    cov = returns.iloc[:, 0].cov(returns.iloc[:, 1])
    var = returns.iloc[:, 1].var()

    return cov / var if var != 0 else np.nan


def load_index_series():
    index_series = download_yahoo_adjclose(INDEX_TICKER, START_DATE, END_DATE)
    if index_series is None:
        raise RuntimeError("Failed to download index data")
    return index_series.dropna().sort_index()


def get_stock_beta(symbol, index_series):
    stock_series = download_yahoo_adjclose(f"{symbol}.NS", START_DATE, END_DATE)
    if stock_series is None:
        return np.nan
    return compute_beta(stock_series, index_series)
