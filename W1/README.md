1주차 정리 
# W1-M3

## GDP ETL Pipeline Overview


해당 과제는 Wikipedia의 국가별 명목 GDP 데이터를 수집하여,
국가–대륙(Region) 정보를 결합한 뒤 정제된 JSON 파일로 저장하는 ETL 파이프라인입니다.

### 1. GDP ETL Pipeline (Wikipedia → JSON)

etl_project_gdp.py

```text
main()
 ├─ load_country_region_map()
 │   └─ country_region_map.json 로드
 │
 ├─ Extract
 │   └─ extract_gdp_table_html()
 │       ├─ fetch_html()
 │       │   └─ Wikipedia HTML 요청
 │       ├─ find_gdp_table_html()
 │       │   └─ GDP 테이블(table HTML) 탐색
 │       └─ _pick_latest_imf_column()
 │           └─ 최신 IMF 연도 컬럼 결정
 │
 ├─ Transform
 │   └─ transform_gdp_table()
 │       ├─ pd.read_html() 로 테이블 로드
 │       ├─ 국가명 정규화 (normalize_country_name)
 │       ├─ GDP 값 정제 및 단위 변환
 │       ├─ 국가 → Region 매핑
 │       ├─ 결측/매핑 실패 데이터 제거
 │       └─ 정렬 및 imf_year 컬럼 추가
 │
 └─ Load
     └─ save_json()
         └─ Countries_by_GDP.json 저장
```
### 2. Database Load & Reporting (JSON → SQLite → Report)

etl_project_gdp_with_sql.py

```text
main()
 ├─ load_from_json()
 │   ├─ Countries_by_GDP.json 로드
 │   ├─ 필수 컬럼 검증
 │   │   └─ country, region, gdp_1b_usd, imf_year
 │   ├─ 타입 변환 및 결측 제거
 │   └─ GDP 내림차순 정렬
 │
 ├─ save_to_db()
 │   ├─ SQLite DB 연결 (World_Economies.db)
 │   ├─ ensure_db_schema()
 │   │   └─ Countries_by_GDP 테이블 생성 (없으면)
 │   ├─ 기존 데이터 삭제 (DELETE)
 │   └─ JSON 기준 데이터 재삽입
 │
 ├─ run_reports()
 │   ├─ print_gdp_over_100b_sql()
 │   │   └─ GDP ≥ 100B USD 국가 목록 출력
 │   └─ print_region_top5_avg_sql()
 │       └─ Region별 Top5 GDP 평균 계산 및 출력
 │
 └─ 로그 기록
     └─ JSON load / DB load / Report start-end





