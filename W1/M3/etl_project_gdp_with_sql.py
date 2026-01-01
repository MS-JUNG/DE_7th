import os
import json
import sqlite3
from datetime import datetime
import pandas as pd


# =============================================================================
# 0) Config
# =============================================================================
DB_PATH = "World_Economies.db"
JSON_PATH = "Countries_by_GDP.json"
LOG_PATH = "etl_project_log.txt"


# =============================================================================
# 1) Logger
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
# 2) JSON -> DataFrame
# =============================================================================
def load_from_json(json_path: str) -> pd.DataFrame:
    """
    JSON 산출물을 DataFrame으로 로드.
    기대 컬럼: country, region, gdp_1b_usd, imf_year
    """
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"'{json_path}' 파일이 없습니다. 먼저 JSON을 생성하세요.")

    with open(json_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    df = pd.DataFrame(records)

    required = ["country", "region", "gdp_1b_usd", "imf_year"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"JSON에 필요한 컬럼이 없습니다: {missing}")

    # 타입 정리
    df["country"] = df["country"].astype(str)
    df["region"] = df["region"].astype(str)
    df["gdp_1b_usd"] = pd.to_numeric(df["gdp_1b_usd"], errors="coerce")
    df["imf_year"] = pd.to_numeric(df["imf_year"], errors="coerce")

    df = df.dropna(subset=["gdp_1b_usd", "imf_year"]).copy()
    df["gdp_1b_usd"] = df["gdp_1b_usd"].astype(float)
    df["imf_year"] = df["imf_year"].astype(int)

    df = df.sort_values("gdp_1b_usd", ascending=False).reset_index(drop=True)
    return df


# =============================================================================
# 3) Load to DB
# =============================================================================
def ensure_db_schema(conn: sqlite3.Connection) -> None:
    conn.execute("""
    CREATE TABLE IF NOT EXISTS Countries_by_GDP (
        Country TEXT NOT NULL,
        Region TEXT,
        GDP_USD_billion REAL NOT NULL
    );
    """)
    conn.commit()


def save_to_db(df: pd.DataFrame, db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        ensure_db_schema(conn)

        # 재실행 시 최신 JSON 기준으로 교체
        conn.execute("DELETE FROM Countries_by_GDP;")
        conn.commit()

        rows = [(r.country, r.region, float(r.gdp_1b_usd)) for r in df.itertuples(index=False)]
        conn.executemany("""
            INSERT INTO Countries_by_GDP (Country, Region, GDP_USD_billion)
            VALUES (?, ?, ?);
        """, rows)
        conn.commit()
    finally:
        conn.close()


# =============================================================================
# 4) Report (SQL)
# =============================================================================
def print_gdp_over_100b_sql(conn: sqlite3.Connection) -> None:
    df = pd.read_sql_query("""
        SELECT
            Country,
            COALESCE(Region, 'Unknown') AS Region,
            GDP_USD_billion
        FROM Countries_by_GDP
        WHERE GDP_USD_billion >= 100.0
        ORDER BY GDP_USD_billion DESC, Country ASC;
    """, conn)

    print("\n=== GDP >= 100B USD Countries ===")
    print(f"count: {len(df)}\n")

    if df.empty:
        print("조건(GDP >= 100B USD)을 만족하는 국가가 없습니다.")
        return

    df_disp = df.copy()
    df_disp["GDP_USD_billion"] = df_disp["GDP_USD_billion"].map(lambda x: f"{x:.2f}")
    print(df_disp.to_string(index=False))


def print_region_top5_avg_sql(conn: sqlite3.Connection) -> None:
    df = pd.read_sql_query("""
        WITH ranked AS (
            SELECT
                COALESCE(Region, 'Unknown') AS Region,
                GDP_USD_billion,
                ROW_NUMBER() OVER (
                    PARTITION BY COALESCE(Region, 'Unknown')
                    ORDER BY GDP_USD_billion DESC
                ) AS rn
            FROM Countries_by_GDP
        )
        SELECT
            Region,
            AVG(GDP_USD_billion) AS Top5_Avg_GDP_USD_billion
        FROM ranked
        WHERE rn <= 5
        GROUP BY Region
        ORDER BY Top5_Avg_GDP_USD_billion DESC;
    """, conn)

    print("\n=== Region별 Top5 GDP 평균 ===\n")
    if df.empty:
        print("계산할 데이터가 없습니다.")
        return

    df_disp = df.copy()
    df_disp["Top5_Avg_GDP_USD_billion"] = df_disp["Top5_Avg_GDP_USD_billion"].map(lambda x: f"{x:.2f}")
    print(df_disp.to_string(index=False))


def run_reports(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        print_gdp_over_100b_sql(conn)
        print_region_top5_avg_sql(conn)
    finally:
        conn.close()


# =============================================================================
# 5) Main 
# =============================================================================
def main():
    log_to_file(LOG_PATH, f"JSON load start ({JSON_PATH})")
    df = load_from_json(JSON_PATH)
    log_to_file(LOG_PATH, f"JSON load end (rows={len(df)})")

    log_to_file(LOG_PATH, "DB load start (Countries_by_GDP)")
    save_to_db(df, DB_PATH)
    log_to_file(LOG_PATH, f"DB load end (DB saved to {DB_PATH})")

    log_to_file(LOG_PATH, "Report start")
    run_reports(DB_PATH)
    log_to_file(LOG_PATH, "Report end")


if __name__ == "__main__":
    main()