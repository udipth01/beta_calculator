import numpy as np
import pandas as pd
import requests
import yfinance as yf
from datetime import datetime, timedelta

def get_nav_data(scheme_code):
    url = f"https://api.mfapi.in/mf/{scheme_code}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json().get("data", [])

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"], format="%d-%m-%Y")
    df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
    df.set_index("date", inplace=True)
    return df["nav"].dropna().sort_index()

def calculate_beta(nav, benchmark):
    df = pd.concat([nav, benchmark], axis=1, join="inner").dropna()
    if len(df) < 60:
        return np.nan
    ret = df.pct_change().dropna()
    return ret.iloc[:,0].cov(ret.iloc[:,1]) / ret.iloc[:,1].var()

def get_mf_beta(scheme_code):
    nav = get_nav_data(scheme_code)

    nifty = yf.download(
        "^NSEI",
        period="2y",
        auto_adjust=True,
        progress=False
    )["Close"]

    return calculate_beta(nav, nifty)

def get_latest_nav(scheme_code):
    url = f"https://api.mfapi.in/mf/{scheme_code}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json().get("data", [])
    
    if not data:
        return None

    latest = data[0]  # MFAPI returns latest first
    return float(latest["nav"])

def get_nav_on_date(scheme_code, target_date):
    url = f"https://api.mfapi.in/mf/{scheme_code}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()

    data = r.json().get("data", [])
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"], format="%d-%m-%Y")
    df["nav"] = pd.to_numeric(df["nav"], errors="coerce")

    df = df.sort_values("date")
    target_date = pd.to_datetime(target_date)

    row = df[df["date"] <= target_date].tail(1)
    if row.empty:
        return None

    return float(row["nav"].iloc[0])
