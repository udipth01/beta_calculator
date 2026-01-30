# isin_master.py
import json
import requests
import urllib3
from zeep import Client, Transport
from functools import lru_cache

ACCESS_TOKEN = "A9fK3M2ZQ7xP4R8W"
WSDL = "https://portfoliohedge.finideas.com/PortFolioPayout/PortfolioService.asmx?WSDL"

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@lru_cache(maxsize=1)
def get_isin_symbol_map():
    """
    Returns: dict { ISIN -> SYMBOL }
    Cached in-memory (process-level)
    """
    session = requests.Session()
    session.verify = False
    transport = Transport(session=session)
    client = Client(wsdl=WSDL, transport=transport)

    result = client.service.Get_EQSymbol(ACCESS_TOKEN)

    try:
        data = json.loads(result)
    except Exception as e:
        raise RuntimeError(f"Failed to parse ISIN master JSON: {e}")

    isin_symbol_map = {}
    if isinstance(data, list):
        for item in data:
            isin = item.get("ISIN", "").strip().upper()
            symbol = item.get("SYMBOL", "").strip().upper()
            if isin and symbol:
                isin_symbol_map[isin] = symbol

    if not isin_symbol_map:
        raise RuntimeError("ISIN master is empty")

    return isin_symbol_map
