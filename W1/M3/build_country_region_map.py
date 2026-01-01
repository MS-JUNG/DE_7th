# build_country_region_map.py
import json
import re
from urllib.parse import urljoin

import pandas as pd
from bs4 import BeautifulSoup

from extract_gdp import fetch_html  
from transform_gdp import normalize_country_name 


def _find_by_region_links(main_html: str, base_url: str):
    """
    return: [(region_name, absolute_url), ...]
    region_name 예: Africa, Arab League, Asia-Pacific, Commonwealth, Latin America and Caribbean,
                 North America, Oceania, Europe
    """
    soup = BeautifulSoup(main_html, "lxml")

    # "By region:" 텍스트가 포함된 구역을 찾고, 그 다음 ul/li/a를 수집
    # 위키 마크업이 조금 바뀌어도: 'By region'을 포함한 텍스트 기준
    by_region_node = None
    for tag in soup.find_all(["h2", "h3", "p"]):
        if tag.get_text(strip=True).lower().startswith("by region"):
            by_region_node = tag
            break

    if by_region_node is None:
    
        # 전체에서 'By region:' 텍스트를 가진 element를 재탐색
        by_region_node = soup.find(string=re.compile(r"By region", re.IGNORECASE))
        if by_region_node:
            by_region_node = by_region_node.parent

    if by_region_node is None:
        raise ValueError("메인 페이지에서 'By region' 섹션을 찾지 못했습니다.")

    # 근처의 ul을 찾기
    ul = by_region_node.find_next("ul")
    if ul is None:
        raise ValueError("'By region' 다음의 ul을 찾지 못했습니다.")

    links = []
    for a in ul.find_all("a", href=True):
        text = a.get_text(" ", strip=True)
        href = a["href"]
        abs_url = urljoin(base_url, href)

        
        #키워드로 region 라벨 결정
        t = text.lower()
        if "afric" in t:
            region = "Africa"
        elif "arab league" in t:
            region = "Arab League"
        elif "asia-pacific" in t or "asia pacific" in t:
            region = "Asia-Pacific"
        elif "commonwealth" in t:
    # Commonwealth of Nations는 지리적 region이 아니므로 제외
            continue
        elif "latin american" in t or "caribbean" in t:
            region = "Latin America & Caribbean"
        elif "north america" in t:
            region = "North America"
        elif "oceanian" in t or "oceania" in t:
            region = "Oceania"
        elif "europe" in t:
            region = "Europe"
        else:
            
            continue

        links.append((region, abs_url))

    # region 중복 제거
    seen = set()
    uniq = []
    for region, url in links:
        if region in seen:
            continue
        seen.add(region)
        uniq.append((region, url))

    if len(uniq) < 5:
        raise ValueError(f"By region 링크 수집이 너무 적습니다. got={uniq}")

    return uniq


def _extract_countries_from_region_page(html: str):
    """
    region 페이지에서 Country 컬럼을 가진 테이블을 찾아 Country 값만 반환
    """
    # read_html로 테이블 Country 컬럼 있는 표 선택
    tables = pd.read_html(html)
    for t in tables:
        country_col = None
        for c in t.columns:
            if "Country" in str(c):
                country_col = c
                break
        if country_col is None:
            continue

        # Country 컬럼만 뽑기
        countries = t[country_col].dropna().astype(str).tolist()
        # normalize 
        countries = [normalize_country_name(x) for x in countries]
        # 빈 값 제거
        countries = [c for c in countries if c]
        return countries

    raise ValueError("region 페이지에서 Country 컬럼이 있는 테이블을 찾지 못했습니다.")


def build_country_region_map(main_url: str, user_agent: str) :
    base_url = "https://en.wikipedia.org"
    main_html = fetch_html(main_url, user_agent=user_agent)

    region_links = _find_by_region_links(main_html, base_url=base_url)

    mapping: dict[str, str] = {}
    for region, url in region_links:
        html = fetch_html(url, user_agent=user_agent)
        countries = _extract_countries_from_region_page(html)

        for c in countries:
            # "먼저 들어온 것을 유지" 
            mapping.setdefault(c, region)

    return mapping


def save_country_region_map(mapping, path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)
