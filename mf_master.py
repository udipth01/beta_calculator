import requests
import pandas as pd
from functools import lru_cache

@lru_cache(maxsize=1)
def get_mf_schemes():
    url = "https://api.mfapi.in/mf"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()

def find_scheme_by_isin(isin):
    isin = isin.strip().upper()
    schemes = get_mf_schemes()

    for s in schemes:
        if isin == str(s.get("isinGrowth", "")).strip():
            return s["schemeCode"], s["schemeName"]

    return None, None
