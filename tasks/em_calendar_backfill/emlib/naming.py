"""市场/资产键命名与候选过滤。

- 键映射仅用于**命名/归并**，不限定市场集合（集合由 discovery 每轮实时发现）。
- 映射未命中 → 用标的代码（小写）兜底 → 新市场自动纳入、不遗漏。
"""
from __future__ import annotations

# 非市场条目（不是可交易市场，不建日历）
EXCLUDE = {
    "CRB",     # 路透 CRB 商品指数
    "BDI",     # 波罗的海干散货
    "HSAHP",   # AH 股溢价（价差，非市场）
}

# 指数代码 → 市场/资产键（仅命名用；缺失一律 code.lower() 兜底）
MAP = {
    # 香港
    "HSI": "hk", "HSCEI": "hk", "HKEXT100": "hk", "HKEXTT": "hk", "HKEXTN": "hk",
    # 美国
    "DJIA": "us", "SPX": "us", "NDX": "us", "NDX100": "us",
    # 日本/韩国/台湾/新加坡/印度/印尼/马来西亚/泰国/越南/菲律宾
    "N225": "jp", "KS11": "kr", "KOSPI200": "kr", "TWII": "tw",
    "STI": "sg", "FSTAS": "sg", "FSTM": "sg",
    "SENSEX": "in", "JKSE": "id", "KLSE": "my", "SET": "th", "VNINDEX": "vn",
    "PSI": "ph", "KSE100": "pk", "CSEALL": "lk",
    # 欧洲
    "FTSE": "uk", "MCX": "uk", "NMX": "uk", "ASX": "uk", "AXX": "uk",
    "GDAXI": "de", "FCHI": "fr", "MIB": "it", "IBEX": "es", "AEX": "nl",
    "BFX": "be", "ATX": "at", "PSI20": "pt", "ASE": "gr", "ISEQ": "ie",
    "HEX": "fi", "HEX25": "fi", "OMXSPI": "se", "OSEBX": "no", "OMXC20": "dk",
    "ICEXI": "is", "WIG": "pl", "PX": "cz", "SSMI": "ch", "SXXP": "eu", "SX5E": "eu",
    # 其它
    "RTS": "ru", "TSX": "ca", "MXX": "mx", "BVSP": "br", "AS51": "au", "AORD": "au",
    "NZ50": "nz", "FISAULMU": "sa",
    # 国内关联
    "XIN9": "cn",
    # 汇率
    "UDI": "fx",
}


def key_of(code: str) -> str | None:
    c = str(code or "").strip()
    if not c or c in EXCLUDE:
        return None
    return MAP.get(c.upper(), c.lower())
