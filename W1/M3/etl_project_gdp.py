
import os
import re
import json

from datetime import datetime
from typing import Dict, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup


# =============================================================================
# 0) Config
# =============================================================================
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
JSON_PATH = "Countries_by_GDP.json"
LOG_PATH = "etl_project_log.txt"
USER_AGENT = "Mozilla/5.0 (compatible; GDP_ETL/1.0)"
COUNTRY_REGION_MAP_PATH = "country_region_map.json"


# =============================================================================
# 1) Logger  (append)
# =============================================================================
def _now_str() -> str:
    return datetime.now().strftime("%Y-%B-%d-%H-%M-%S")


def log_to_file(log_path: str, msg: str, also_print: bool = True) -> None:
    line = f"{_now_str()}, {msg}\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line)
    if also_print:
        print(line.rstrip())


# =============================================================================
# 2) Extract
# =============================================================================
def fetch_html(url: str, user_agent: str, timeout: int = 30) -> str:
    headers = {"User-Agent": user_agent}
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def _pick_latest_imf_column(columns) -> Tuple[str, int]:
    """
    컬럼들 중 'IMF (YYYY)' 형태를 찾아 가장 최신 연도 컬럼을 반환
    return: (imf_col_name, imf_year)
    """
    imf_cols = []
    for c in columns:
        m = re.search(r"\bIMF\b.*\((\d{4})\)", str(c))
        if m:
            imf_cols.append((int(m.group(1)), c))

    if not imf_cols:
        raise ValueError("IMF (YYYY) 컬럼을 찾지 못했습니다.")

    imf_cols.sort(reverse=True, key=lambda x: x[0])
    return imf_cols[0][1], imf_cols[0][0]


def find_gdp_table_html(html: str) -> str:
    """
    BS4로 table 태그들을 순회하며,
    'Country' 컬럼과 'IMF (YYYY)' 컬럼을 동시에 가진 테이블을 찾아 반환
    """
    soup = BeautifulSoup(html, "lxml")
    table_tags = soup.find_all("table")

    for table_tag in table_tags:
        try:
            dfs = pd.read_html(str(table_tag))
            if not dfs:
                continue
            t = dfs[0]

            if any("Country" in str(c) for c in t.columns):
                _col, _year = _pick_latest_imf_column(t.columns)  # IMF 컬럼 검증
                return str(table_tag)
        except Exception:
            continue

    raise ValueError("GDP 테이블을 찾지 못했습니다.")


def extract_gdp_table_html(url: str, user_agent: str) -> str:
    html = fetch_html(url, user_agent=user_agent)
    return find_gdp_table_html(html)


# =============================================================================
# 3) Transform
# =============================================================================
def normalize_country_name(name: str) -> str:
    name = re.sub(r"\[.*?\]", "", str(name)).strip()
    name = re.sub(r"\(.*?\)", "", name).strip()
    return name


def load_country_region_map(path: str) -> Dict[str, str]:
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"'{path}' 파일이 없습니다.\n"
            f"- 해결: 같은 폴더에 {path}를 두거나, COUNTRY_REGION_MAP_PATH를 올바르게 설정하세요."
        )
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def transform_gdp_table(table_html: str, country_region_map: Dict[str, str]) -> pd.DataFrame:
    """
    반환 컬럼:
      country, region, gdp_1b_usd, imf_year
    정렬:
      gdp_1b_usd 내림차순
    """
    t = pd.read_html(table_html)[0]

    # 최신 IMF 컬럼 선택
    imf_col, imf_year = _pick_latest_imf_column(t.columns)

    # Country 컬럼 찾기
    country_col = None
    for c in t.columns:
        if "Country" in str(c):
            country_col = c
            break
    if country_col is None:
        raise ValueError("Country 컬럼을 찾지 못했습니다.")

    df = t[[country_col, imf_col]].copy()
    df.columns = ["country_raw", "gdp_million_usd_raw"]

    # 국가명 정규화
    df["country"] = df["country_raw"].apply(normalize_country_name)

    # GDP 숫자 정리 (million USD -> 1B USD)
    s = (
        df["gdp_million_usd_raw"].astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("—", "", regex=False)
        .str.replace("N/A", "", regex=False)
        .str.strip()
    )
    df["gdp_million_usd"] = pd.to_numeric(s, errors="coerce")
    df["gdp_1b_usd"] = (df["gdp_million_usd"] / 1000.0).round(2)
    df = df.dropna(subset=["gdp_1b_usd"]).copy()

    # Region 매핑 (매핑 실패는 제거)
    df["region"] = df["country"].map(country_region_map)
    df = df[df["region"].notna()].copy()

    # GDP 높은 국가가 먼저 나오도록 정렬
    df = df.sort_values("gdp_1b_usd", ascending=False).reset_index(drop=True)
    df["imf_year"] = int(imf_year)

    return df[["country", "region", "gdp_1b_usd", "imf_year"]]


# =============================================================================
# 4) Load (JSON only)
# =============================================================================
def save_json(df: pd.DataFrame, json_path: str) -> None:
    records = df.to_dict(orient="records")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


# =============================================================================
# 5) Main (JSON 생성까지)
# =============================================================================
def main():
    # Region 매핑 로드
    country_region_map = load_country_region_map(COUNTRY_REGION_MAP_PATH)

    # =========================
    # ETL: Extract
    # =========================
    log_to_file(LOG_PATH, "ETL Extract start")
    table_html = extract_gdp_table_html(WIKI_URL, USER_AGENT)
    log_to_file(LOG_PATH, "ETL Extract end")

    # =========================
    # ETL: Transform
    # =========================
    log_to_file(LOG_PATH, "ETL Transform start")
    df = transform_gdp_table(table_html, country_region_map)
    log_to_file(LOG_PATH, f"ETL Transform end (rows={len(df)})")

    # =========================
    # ETL: Load (JSON)
    # =========================
    log_to_file(LOG_PATH, "ETL Load start (JSON)")
    save_json(df, JSON_PATH)
    log_to_file(LOG_PATH, f"ETL Load end (JSON saved to {JSON_PATH})")


if __name__ == "__main__":
    main()