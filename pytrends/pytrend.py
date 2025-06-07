import pandas as pd
from pytrends.request import TrendReq
from datetime import timedelta
import time
import random
import csv
import re

# 제목 정리: 괄호 및 특수문자 제거
def clean_title(title):
    title = re.sub(r"\(.*?\)", "", title)
    title = re.sub(r"[^\w\s가-힣]", "", title)
    return title.strip()

# 키워드 추출: 불용어 제거, 특수문자 제거, 길이 필터링
def extract_keywords(title):
    stop_words = {
        'a', 'an', 'the', 'in', 'on', 'at', 'to', 'of', 'with', 'by', 
        'for', 'and', 'or', 'from', 'as', 'but'
    }
    cleaned = re.sub(r'[^\w\s]', '', title)  # 특수문자 제거
    words = cleaned.split()
    return list(set([title] + [w for w in words if w.lower() not in stop_words]))

# CSV 로딩
csv_path = "/Users/yoonjeonghun/Desktop/실습/3-1/기계학습/전처리/movies_2015_plus_full_filtered.csv"
df = pd.read_csv(csv_path)
df = df[['title', 'release_date']].dropna()
df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
df = df.dropna(subset=['release_date'])
df['start_date'] = df['release_date'] - timedelta(days=14)
df['end_date'] = df['release_date'] + timedelta(days=14)

# 출력 컬럼 이름
outcols = ['title', 'date'] + [f'd{n}' if n < 0 else ('d' if n == 0 else f'd+{n}') for n in range(-14, 15)]

# 실행할 영화 구간
batch_size = 100
block_num = int(input(f"\n몇 번째 100개 구간을 실행할까요? (예: 1 → 1~100, 2 → 101~200): ").strip())
start_idx = (block_num - 1) * batch_size
# end_idx = min(start_idx + batch_size, len(df))
end_idx = min(start_idx + 10, len(df))  # 테스트용으로 10개만 실행

target_df = df.iloc[start_idx:end_idx].reset_index(drop=True)
print(f"\n[{start_idx+1}번 ~ {end_idx}번 영화까지 진행]")

# pytrends 객체 생성
pytrends = TrendReq(hl='ko', tz=540)
results = []
file_num = block_num

# 메인 루프
for index, row in target_df.iterrows():
    release_date = row['release_date']
    raw_title = row['title']
    title = clean_title(raw_title)
    start = row['start_date'].strftime('%Y-%m-%d')
    end = row['end_date'].strftime('%Y-%m-%d')

    print(f"[{start_idx + index + 1}/{len(df)}] 요청 중: {title} ({start} ~ {end})")

    keywords = extract_keywords(title)
    print(f"  - 검색어 목록: {keywords}")
    series_list = []

    for kw in keywords:
        try:
            pytrends.build_payload([kw], timeframe=f"{start} {end}", geo='KR')
            data = pytrends.interest_over_time()
            if not data.empty:
                series = data[kw]
                series_list.append(series)
        except Exception as e:
            print(f"  - 무시됨: {kw} → {e}")
            continue
        time.sleep(random.uniform(1.0, 3.0))

    # series_list가 비어있으면 0으로 채운 시계열, 아니면 평균
    if not series_list:
        print(f"[{title}] → 검색 결과 없음")
        timeline_df = pd.Series([0]*29, index=pd.date_range(start=start, end=end))  # 0으로 채운 시계열
    else:
        timeline_df = pd.concat(series_list, axis=1).mean(axis=1)

    # 결과 저장
    row_result = {
        'title': raw_title,
        'date': release_date.strftime('%Y-%m-%d'),
    }
    for i, value in enumerate(timeline_df):
        key = f'd{(i - 14) if (i - 14) < 0 else ("+" + str(i - 14)) if (i - 14) > 0 else ""}'
        row_result[key] = int(value)

    results.append(row_result)

    out_file = f"pytrends{file_num}.csv"
    with open(out_file, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=outcols)
        writer.writeheader()
        for r in results:
            writer.writerow({col: r.get(col, 0) for col in outcols})

print(f"\n완료! pytrends{file_num}.csv로 저장되었습니다.")