"""CorrelationEngine — maps entities to tradeable instruments.

200+ mappings covering equities, ADRs, commodities, FX, crypto,
sector/country/bond ETFs, and index futures.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


# ── Master ticker map ──────────────────────────────────────────────────
# Maps entity names (lowercase) → primary ticker

ENTITY_TICKER_MAP: dict[str, str] = {
    # ── US Mega-caps / FAANG+ ──────────────────────────────────────────
    "apple": "AAPL", "apple inc": "AAPL", "apple inc.": "AAPL",
    "microsoft": "MSFT", "microsoft corp": "MSFT", "microsoft corporation": "MSFT",
    "amazon": "AMZN", "amazon.com": "AMZN", "amazon.com inc": "AMZN",
    "google": "GOOG", "alphabet": "GOOG", "alphabet inc": "GOOG", "google/alphabet": "GOOG",
    "meta": "META", "meta platforms": "META", "facebook": "META",
    "nvidia": "NVDA", "nvidia corp": "NVDA", "nvidia corp.": "NVDA", "nvidia corporation": "NVDA",
    "tesla": "TSLA", "tesla inc": "TSLA", "tesla inc.": "TSLA",
    "netflix": "NFLX", "netflix inc": "NFLX",
    "broadcom": "AVGO", "broadcom inc": "AVGO",
    # ── US Tech ────────────────────────────────────────────────────────
    "amd": "AMD", "advanced micro devices": "AMD",
    "intel": "INTC", "intel corp": "INTC", "intel corp.": "INTC", "intel corporation": "INTC",
    "qualcomm": "QCOM", "qualcomm inc": "QCOM",
    "salesforce": "CRM", "salesforce inc": "CRM",
    "adobe": "ADBE", "adobe inc": "ADBE",
    "oracle": "ORCL", "oracle corp": "ORCL",
    "servicenow": "NOW",
    "snowflake": "SNOW",
    "palantir": "PLTR", "palantir technologies": "PLTR",
    "crowdstrike": "CRWD", "crowdstrike holdings": "CRWD",
    "palo alto networks": "PANW",
    "uber": "UBER", "uber technologies": "UBER",
    "airbnb": "ABNB",
    "spotify": "SPOT",
    "snap": "SNAP", "snapchat": "SNAP",
    "pinterest": "PINS",
    "shopify": "SHOP",
    "zoom": "ZM", "zoom video": "ZM",
    "docusign": "DOCU",
    "twilio": "TWLO",
    "datadog": "DDOG",
    "mongodb": "MDB",
    "cloudflare": "NET",
    "asml": "ASML", "asml holding": "ASML",
    "arm": "ARM", "arm holdings": "ARM",
    "micron": "MU", "micron technology": "MU",
    "texas instruments": "TXN",
    "applied materials": "AMAT",
    "lam research": "LRCX",
    "kla": "KLAC", "kla corporation": "KLAC",
    "marvell": "MRVL", "marvell technology": "MRVL",
    "synopsys": "SNPS",
    "cadence": "CDNS", "cadence design": "CDNS",
    # ── US Banks & Finance ─────────────────────────────────────────────
    "jpmorgan": "JPM", "jpmorgan chase": "JPM", "jp morgan": "JPM",
    "bank of america": "BAC", "bofa": "BAC",
    "wells fargo": "WFC",
    "citigroup": "C", "citi": "C",
    "goldman sachs": "GS",
    "morgan stanley": "MS",
    "charles schwab": "SCHW",
    "blackrock": "BLK",
    "berkshire hathaway": "BRK.B", "berkshire": "BRK.B",
    "visa": "V",
    "mastercard": "MA",
    "american express": "AXP", "amex": "AXP",
    "paypal": "PYPL",
    "block": "SQ", "square": "SQ",
    "robinhood": "HOOD",
    "coinbase": "COIN",
    "new york community bancorp": "NYCB", "nycb": "NYCB",
    # ── US Industrials ─────────────────────────────────────────────────
    "boeing": "BA", "boeing co": "BA", "boeing co.": "BA",
    "lockheed martin": "LMT",
    "raytheon": "RTX", "rtx": "RTX",
    "general electric": "GE", "ge": "GE",
    "honeywell": "HON",
    "caterpillar": "CAT",
    "3m": "MMM",
    "deere": "DE", "john deere": "DE",
    "general dynamics": "GD",
    "northrop grumman": "NOC",
    "union pacific": "UNP",
    "ups": "UPS", "united parcel service": "UPS",
    "fedex": "FDX",
    # ── US Healthcare & Pharma ─────────────────────────────────────────
    "unitedhealth": "UNH", "unitedhealth group": "UNH",
    "johnson & johnson": "JNJ", "j&j": "JNJ",
    "pfizer": "PFE", "pfizer inc": "PFE", "pfizer inc.": "PFE",
    "eli lilly": "LLY", "lilly": "LLY",
    "abbvie": "ABBV",
    "merck": "MRK",
    "bristol-myers squibb": "BMY", "bms": "BMY",
    "amgen": "AMGN",
    "gilead": "GILD", "gilead sciences": "GILD",
    "moderna": "MRNA",
    "novo nordisk": "NVO",
    "regeneron": "REGN",
    "vertex": "VRTX", "vertex pharmaceuticals": "VRTX",
    # ── US Consumer & Retail ───────────────────────────────────────────
    "walmart": "WMT", "walmart inc": "WMT", "walmart inc.": "WMT",
    "costco": "COST",
    "target": "TGT",
    "home depot": "HD",
    "lowes": "LOW", "lowe's": "LOW",
    "nike": "NKE", "nike inc": "NKE", "nike inc.": "NKE",
    "starbucks": "SBUX",
    "mcdonalds": "MCD", "mcdonald's": "MCD",
    "coca-cola": "KO", "coke": "KO",
    "pepsi": "PEP", "pepsico": "PEP",
    "procter & gamble": "PG", "p&g": "PG",
    "disney": "DIS", "walt disney": "DIS", "walt disney company": "DIS",
    "lululemon": "LULU",
    "chipotle": "CMG",
    "peloton": "PTON",
    "dollar general": "DG",
    "ross stores": "ROST",
    "tj maxx": "TJX", "tjx": "TJX",
    # ── US Energy ──────────────────────────────────────────────────────
    "exxon": "XOM", "exxonmobil": "XOM", "exxon mobil": "XOM",
    "chevron": "CVX",
    "conocophillips": "COP",
    "schlumberger": "SLB",
    "halliburton": "HAL",
    "pioneer natural resources": "PXD",
    "devon energy": "DVN",
    "marathon petroleum": "MPC",
    "valero": "VLO",
    "nextera energy": "NEE",
    # ── US Real Estate & Utilities ─────────────────────────────────────
    "prologis": "PLD",
    "american tower": "AMT",
    "crown castle": "CCI",
    "equinix": "EQIX",
    "realty income": "O",
    "simon property": "SPG",
    "duke energy": "DUK",
    "southern company": "SO",
    # ── US Telecom & Media ─────────────────────────────────────────────
    "at&t": "T",
    "verizon": "VZ",
    "t-mobile": "TMUS",
    "comcast": "CMCSA",
    "warner bros discovery": "WBD",
    "paramount": "PARA",
    # ── International ADRs ─────────────────────────────────────────────
    "tsmc": "TSM", "taiwan semiconductor": "TSM",
    "samsung": "005930.KS", "samsung electronics": "005930.KS",
    "toyota": "TM", "toyota motor": "TM",
    "sony": "SONY",
    "lvmh": "MC.PA",
    "shell": "SHEL", "shell plc": "SHEL",
    "bp": "BP",
    "totalenergies": "TTE",
    "nestle": "NSRGY",
    "roche": "RHHBY",
    "novartis": "NVS",
    "sap": "SAP",
    "siemens": "SIEGY",
    "alibaba": "BABA",
    "tencent": "TCEHY",
    "baidu": "BIDU",
    "jd.com": "JD", "jd": "JD",
    "pinduoduo": "PDD", "temu": "PDD",
    "byd": "BYDDF", "byd company": "BYDDF",
    "nio": "NIO",
    "xpeng": "XPEV",
    "li auto": "LI",
    "sea limited": "SE",
    "grab": "GRAB",
    "mercadolibre": "MELI",
    "nubank": "NU",
    "saudi aramco": "2222.SR",
    "reliance industries": "RELIANCE.NS",
    "infosys": "INFY",
    "hdfc bank": "HDB",
    # ── Commodities (futures symbols) ──────────────────────────────────
    "crude oil": "CL", "oil": "CL", "wti": "CL", "brent": "BZ",
    "gold": "GC", "gold price": "GC",
    "silver": "SI",
    "copper": "HG", "copper price": "HG",
    "natural gas": "NG", "nat gas": "NG",
    "wheat": "ZW",
    "corn": "ZC",
    "soybeans": "ZS",
    "cotton": "CT",
    "coffee": "KC",
    "sugar": "SB",
    "lumber": "LBS",
    "platinum": "PL",
    "palladium": "PA",
    "iron ore": "TIO",
    "lithium": "LIT",
    "uranium": "URA",
    # ── FX pairs ───────────────────────────────────────────────────────
    "us dollar": "DXY", "usd": "DXY", "dollar index": "DXY",
    "euro": "EUR/USD", "eur/usd": "EUR/USD",
    "japanese yen": "USD/JPY", "yen": "USD/JPY", "usd/jpy": "USD/JPY",
    "chinese yuan": "USD/CNY", "yuan": "USD/CNY", "renminbi": "USD/CNY", "usd/cny": "USD/CNY", "usd/cnh": "USD/CNH",
    "british pound": "GBP/USD", "pound sterling": "GBP/USD", "gbp/usd": "GBP/USD",
    "swiss franc": "USD/CHF", "usd/chf": "USD/CHF",
    "australian dollar": "AUD/USD", "aussie dollar": "AUD/USD",
    "canadian dollar": "USD/CAD",
    # ── Crypto ─────────────────────────────────────────────────────────
    "bitcoin": "BTC", "btc": "BTC",
    "ethereum": "ETH", "eth": "ETH",
    "solana": "SOL", "sol": "SOL",
    "xrp": "XRP", "ripple": "XRP",
    "cardano": "ADA",
    "dogecoin": "DOGE",
    "polkadot": "DOT",
    "avalanche": "AVAX",
    "chainlink": "LINK",
    "polygon": "MATIC",
    "uniswap": "UNI",
    # ── Sector ETFs ────────────────────────────────────────────────────
    "financial sector": "XLF", "financials": "XLF", "financials sector": "XLF",
    "energy sector": "XLE", "energy": "XLE",
    "technology sector": "XLK", "tech sector": "XLK",
    "healthcare sector": "XLV", "healthcare": "XLV",
    "consumer discretionary": "XLY",
    "consumer staples": "XLP",
    "industrial sector": "XLI", "industrials": "XLI",
    "materials sector": "XLB",
    "real estate sector": "XLRE",
    "utilities sector": "XLU", "utilities": "XLU",
    "communication services": "XLC",
    "semiconductors": "SMH", "semiconductor sector": "SMH", "chip sector": "SMH",
    "retail sector": "XRT", "retail": "XRT",
    "homebuilders": "XHB", "housing sector": "XHB",
    "biotech sector": "XBI", "biotech": "XBI",
    "aerospace & defense": "ITA", "defense sector": "ITA",
    "regional banks": "KRE",
    "oil services": "OIH",
    "clean energy": "ICLN", "renewable energy": "ICLN",
    "cybersecurity": "HACK",
    "cloud computing": "SKYY",
    "ai etf": "BOTZ", "robotics": "BOTZ",
    # ── Country / Region ETFs ──────────────────────────────────────────
    "japan": "EWJ",
    "china": "FXI", "china stocks": "FXI",
    "brazil": "EWZ",
    "germany": "EWG",
    "india": "INDA",
    "south korea": "EWY",
    "united kingdom": "EWU", "uk": "EWU",
    "australia": "EWA",
    "mexico": "EWW",
    "taiwan": "EWT",
    "emerging markets": "EEM", "em": "EEM",
    "europe": "VGK", "european union": "VGK",
    "frontier markets": "FM",
    # ── Index futures ──────────────────────────────────────────────────
    "s&p 500": "ES", "sp500": "ES", "spx": "ES", "spy": "SPY",
    "nasdaq": "NQ", "nasdaq 100": "NQ", "qqq": "QQQ",
    "dow jones": "YM", "djia": "YM", "dow": "YM",
    "russell 2000": "RTY", "small caps": "RTY", "iwm": "IWM",
    "vix": "VIX", "volatility": "VIX",
    # ── Bond ETFs ──────────────────────────────────────────────────────
    "treasury bonds": "TLT", "long bonds": "TLT", "treasuries": "TLT",
    "high yield bonds": "HYG", "junk bonds": "HYG", "high yield": "HYG",
    "investment grade bonds": "LQD", "ig bonds": "LQD",
    "total bond market": "BND",
    "tips": "TIP", "inflation protected": "TIP",
    "short-term treasury": "SHY",
    "10-year treasury": "IEF", "10y treasury": "IEF",
    "municipal bonds": "MUB",
}

# ── Related instruments map ────────────────────────────────────────────
# ticker → list of related instruments

_RELATED: dict[str, list[dict[str, str]]] = {
    "AAPL": [
        {"symbol": "XLK", "relationship": "sector_etf"},
        {"symbol": "SMH", "relationship": "component_exposure"},
        {"symbol": "QQQ", "relationship": "index_etf"},
        {"symbol": "TSM", "relationship": "supplier"},
        {"symbol": "MSFT", "relationship": "competitor"},
        {"symbol": "GOOG", "relationship": "competitor"},
    ],
    "TSLA": [
        {"symbol": "XLY", "relationship": "sector_etf"},
        {"symbol": "BYDDF", "relationship": "competitor"},
        {"symbol": "NIO", "relationship": "competitor"},
        {"symbol": "LIT", "relationship": "supply_chain"},
        {"symbol": "QQQ", "relationship": "index_etf"},
    ],
    "NVDA": [
        {"symbol": "SMH", "relationship": "sector_etf"},
        {"symbol": "AMD", "relationship": "competitor"},
        {"symbol": "TSM", "relationship": "manufacturer"},
        {"symbol": "AVGO", "relationship": "peer"},
        {"symbol": "BOTZ", "relationship": "theme_etf"},
    ],
    "BA": [
        {"symbol": "ITA", "relationship": "sector_etf"},
        {"symbol": "XLI", "relationship": "sector_etf"},
        {"symbol": "EADSY", "relationship": "competitor"},
        {"symbol": "GE", "relationship": "supplier"},
        {"symbol": "RTX", "relationship": "peer"},
    ],
    "JPM": [
        {"symbol": "XLF", "relationship": "sector_etf"},
        {"symbol": "KRE", "relationship": "sector_etf"},
        {"symbol": "BAC", "relationship": "competitor"},
        {"symbol": "GS", "relationship": "competitor"},
    ],
    "CL": [
        {"symbol": "XLE", "relationship": "sector_etf"},
        {"symbol": "XOM", "relationship": "producer"},
        {"symbol": "CVX", "relationship": "producer"},
        {"symbol": "OIH", "relationship": "services"},
        {"symbol": "XLY", "relationship": "inverse_consumer"},
    ],
    "GC": [
        {"symbol": "GDX", "relationship": "miners_etf"},
        {"symbol": "SLV", "relationship": "related_metal"},
        {"symbol": "TLT", "relationship": "safe_haven_peer"},
        {"symbol": "DXY", "relationship": "inverse_correlation"},
    ],
    "BTC": [
        {"symbol": "ETH", "relationship": "correlated_crypto"},
        {"symbol": "COIN", "relationship": "exchange"},
        {"symbol": "MSTR", "relationship": "proxy"},
        {"symbol": "SOL", "relationship": "alt_chain"},
    ],
    "TSM": [
        {"symbol": "SMH", "relationship": "sector_etf"},
        {"symbol": "AAPL", "relationship": "customer"},
        {"symbol": "NVDA", "relationship": "customer"},
        {"symbol": "AMD", "relationship": "customer"},
        {"symbol": "INTC", "relationship": "competitor"},
        {"symbol": "ASML", "relationship": "equipment_supplier"},
        {"symbol": "EWT", "relationship": "country_etf"},
    ],
    "META": [
        {"symbol": "XLC", "relationship": "sector_etf"},
        {"symbol": "GOOG", "relationship": "competitor"},
        {"symbol": "SNAP", "relationship": "competitor"},
        {"symbol": "PINS", "relationship": "competitor"},
    ],
    "MSFT": [
        {"symbol": "XLK", "relationship": "sector_etf"},
        {"symbol": "GOOG", "relationship": "competitor"},
        {"symbol": "CRM", "relationship": "competitor"},
        {"symbol": "AAPL", "relationship": "competitor"},
    ],
    "AMZN": [
        {"symbol": "XLY", "relationship": "sector_etf"},
        {"symbol": "SHOP", "relationship": "competitor"},
        {"symbol": "WMT", "relationship": "competitor"},
        {"symbol": "MSFT", "relationship": "cloud_competitor"},
    ],
    "XOM": [
        {"symbol": "XLE", "relationship": "sector_etf"},
        {"symbol": "CL", "relationship": "commodity"},
        {"symbol": "CVX", "relationship": "competitor"},
        {"symbol": "COP", "relationship": "peer"},
    ],
    "PFE": [
        {"symbol": "XLV", "relationship": "sector_etf"},
        {"symbol": "XBI", "relationship": "sector_etf"},
        {"symbol": "MRNA", "relationship": "competitor"},
        {"symbol": "JNJ", "relationship": "peer"},
    ],
    "NKE": [
        {"symbol": "XLY", "relationship": "sector_etf"},
        {"symbol": "LULU", "relationship": "competitor"},
        {"symbol": "ADDYY", "relationship": "competitor"},
    ],
    "WMT": [
        {"symbol": "XLP", "relationship": "sector_etf"},
        {"symbol": "XRT", "relationship": "sector_etf"},
        {"symbol": "TGT", "relationship": "competitor"},
        {"symbol": "COST", "relationship": "competitor"},
    ],
    "HG": [
        {"symbol": "COPX", "relationship": "miners_etf"},
        {"symbol": "XLB", "relationship": "materials_etf"},
        {"symbol": "BHP", "relationship": "producer"},
        {"symbol": "FCX", "relationship": "producer"},
    ],
    "SPY": [
        {"symbol": "QQQ", "relationship": "related_index"},
        {"symbol": "IWM", "relationship": "related_index"},
        {"symbol": "VIX", "relationship": "volatility"},
        {"symbol": "TLT", "relationship": "inverse_correlation"},
    ],
}


class CorrelationEngine:
    """Maps entity names to tradeable instruments and finds related instruments."""

    def resolve_ticker(self, entity_name: str, entity_type: str = "") -> str | None:
        """Fuzzy-match an entity name to a ticker symbol."""
        key = entity_name.lower().strip()
        if key in ENTITY_TICKER_MAP:
            return ENTITY_TICKER_MAP[key]

        # Try partial match
        for map_key, ticker in ENTITY_TICKER_MAP.items():
            if key in map_key or map_key in key:
                return ticker

        return None

    def get_related_instruments(self, ticker: str) -> list[dict[str, str]]:
        """Return related instruments for a given ticker."""
        if not ticker:
            return []
        return _RELATED.get(ticker.upper(), [])

    def get_tradeable_instruments(self, entity_name: str, entity_type: str = "", ticker: str | None = None) -> list[dict[str, Any]]:
        """Return all ways to trade this entity: direct, sector ETF, country ETF, etc."""
        resolved = ticker or self.resolve_ticker(entity_name, entity_type)
        if not resolved:
            return []

        instruments: list[dict[str, Any]] = []

        # Direct instrument
        asset_class = "equity"
        if entity_type == "COMMODITY":
            asset_class = "commodity"
        elif entity_type == "CRYPTO":
            asset_class = "crypto"
        elif entity_type == "COUNTRY":
            asset_class = "etf"
        elif entity_type == "SECTOR":
            asset_class = "etf"
        elif "/" in resolved:
            asset_class = "fx"

        instruments.append({
            "symbol": resolved,
            "asset_class": asset_class,
            "type": "direct",
            "entity": entity_name,
        })

        # Related instruments
        for related in self.get_related_instruments(resolved):
            rel_type = related.get("relationship", "related")
            instruments.append({
                "symbol": related["symbol"],
                "asset_class": "etf" if related["symbol"].isupper() and len(related["symbol"]) <= 4 else "equity",
                "type": rel_type,
                "entity": entity_name,
            })

        return instruments
