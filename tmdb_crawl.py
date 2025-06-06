"""
movies_2015_plus_full.py  ★제작국가 컬럼 추가판
────────────────────────────────────────────
• 대상: 2015-01-01 이후 개봉, 제작국가 KR · US · JP
• MIN_VOTES 조정 가능 (인기도 필터)
• 저장: title, release_date, imdb_id, tmdb_id, overview
         origin_country, directors, cast_top5, genres,
         production_companies, production_countries
"""

from __future__ import annotations
import asyncio, csv, itertools, os, sys
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp
from aiohttp import ClientSession
from dotenv import load_dotenv
from tqdm.asyncio import tqdm_asyncio

# ── Windows 경고 억제(선택) ────────────────────
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ── 환경 변수 ───────────────────────────────────
load_dotenv()
API_KEY = os.getenv("TMDB_API_KEY") or "YOUR_KEY_HERE"
if API_KEY == "YOUR_KEY_HERE":
    sys.exit("❌  TMDB_API_KEY 설정 필요")

# ── 수집 파라미터 ───────────────────────────────
COUNTRIES        = ["KR", "US", "JP"]
START_DATE       = "2015-01-01"
END_DATE         = date.today().isoformat()
MIN_VOTES        = 50          # 인기도 필터 (0이면 해제)
CONCURRENT_REQ   = 40          # 무료 리밋: 40 req / 10 s
CSV_FILE         = Path("movies_2015_plus_full_kr.csv")
OVERVIEW_LANG    = "ko-KR"

HEADERS = {
    "User-Agent": "tmdb-2015plus-scraper/2.1",
    "Accept"    : "application/json;charset=utf-8",
}

DISCOVER = "https://api.themoviedb.org/3/discover/movie"
DETAIL   = "https://api.themoviedb.org/3/movie/{id}"

# ── 공통 GET + 429 재시도 ────────────────────────
async def tmdb_get(session: ClientSession, url: str,
                   params: dict, retry: int = 3) -> Optional[dict]:
    params["api_key"] = API_KEY
    for _ in range(retry):
        async with session.get(url, params=params) as r:
            if r.status == 429:
                await asyncio.sleep(int(r.headers.get("Retry-After", 2)))
                continue
            if r.status != 200:
                return None
            return await r.json()
    return None

# ── 1) 제작국가별 discover ───────────────────────
async def discover_country(session: ClientSession,
                           country: str) -> List[Tuple[int, str]]:
    """해당 제작국가 모든 영화 (tmdb_id, origin_country)"""
    ids, page = [], 1
    while True:
        js = await tmdb_get(session, DISCOVER, {
            "with_origin_country"      : country,
            "primary_release_date.gte" : START_DATE,
            "primary_release_date.lte" : END_DATE,
            "vote_count.gte"           : MIN_VOTES,
            "sort_by"                  : "primary_release_date.asc",
            "page"                     : page,
        })
        if not js or not js["results"]:
            break
        ids += [(m["id"], country) for m in js["results"]]
        if page >= js["total_pages"]:
            break
        page += 1
        await asyncio.sleep(0.15)
    return ids

# ── 2) 상세 + credits ────────────────────────────
async def fetch_detail(session: ClientSession, tmid: int,
                       origin: str) -> Dict:
    js = await tmdb_get(session, DETAIL.format(id=tmid),
                        {"language": OVERVIEW_LANG,
                         "append_to_response": "credits"})
    if not js:
        return {}

    directors = [p["name"] for p in js["credits"]["crew"]
                 if p.get("job") == "Director"]
    cast      = [c["name"] for c in js["credits"]["cast"][:5]]

    return {
        "title"                : js.get("title", ""),
        "release_date"         : js.get("release_date", ""),
        "imdb_id"              : js.get("imdb_id", ""),
        "tmdb_id"              : tmid,
        "overview"             : (js.get("overview") or "").replace("\n", " ").strip(),
        "origin_country"       : origin,
        "directors"            : ", ".join(directors),
        "cast_top5"            : ", ".join(cast),
        "genres"               : ", ".join(g["name"] for g in js.get("genres", [])),
        "production_companies" : ", ".join(c["name"] for c in js.get("production_companies", [])),
        "production_countries" : ", ".join(c["iso_3166_1"] for c in js.get("production_countries", [])),
    }

# ── 메인 ─────────────────────────────────────────
async def main():
    connector = aiohttp.TCPConnector(limit=CONCURRENT_REQ)
    async with aiohttp.ClientSession(headers=HEADERS,
                                     connector=connector) as session:
        # 1) discover
        discover_tasks = [discover_country(session, c) for c in COUNTRIES]
        lists = await tqdm_asyncio.gather(*discover_tasks,
                                          desc="(1/2) Discover pages")

        # (tmdb_id, origin) 중복 처리 → origin 합치기
        origin_map: Dict[int, set] = {}
        for pair_list in lists:
            for tid, oc in pair_list:
                origin_map.setdefault(tid, set()).add(oc)

        unique_pairs = [(tid, ",".join(sorted(ocs)))
                        for tid, ocs in origin_map.items()]
        print(f"▶ 대상 영화(고유): {len(unique_pairs):,}편")

        # 2) 상세
        tasks_detail = [fetch_detail(session, tid, origin)
                        for tid, origin in unique_pairs]
        rows = await tqdm_asyncio.gather(*tasks_detail,
                                         desc="(2/2) Fetch details")

    # 3) CSV 저장
    fields = ["title", "release_date", "imdb_id", "tmdb_id", "overview",
              "origin_country", "directors", "cast_top5", "genres",
              "production_companies", "production_countries"]

    with CSV_FILE.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows([r for r in rows if r])

    ok = sum(bool(r) for r in rows)
    print(f"\n✅ CSV 완료 → {CSV_FILE.resolve()}  (저장 {ok:,}편)")

# ── 실행 ───────────────────────────────────────
if __name__ == "__main__":
    asyncio.run(main())
