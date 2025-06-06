import pandas as pd
from serpapi import GoogleSearch
from datetime import timedelta
import time
import csv

# API KEY 직접 입력 (예시)
API_KEY =  "fd05c359fd2317bf5b263ef68d3b79012a41d1469a2d99f1ee1b662b1ea3fae4"
# "6c887f3b93c9855fc5068d60fdc3a3b5b6fa8febb92de51ff6c83641d5bf5326",
# "f45c466816ad21a6324979f9b9ceb289c5a0eddd5ffb83a84030a1e6e292b4f4",


csv_path = "/Users/yoonjeonghun/Desktop/실습/3-1/기계학습/전처리/movies_2015_plus_full_filtered.csv"

df = pd.read_csv(csv_path)
df = df[['title', 'release_date']].dropna()
df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
df = df.dropna(subset=['release_date'])
df['start_date'] = df['release_date'] - timedelta(days=14)
df['end_date'] = df['release_date'] + timedelta(days=14)

outcols = ['title', 'date'] + [f'd{n}' if n < 0 else ('d' if n == 0 else f'd+{n}') for n in range(-14, 15)]

batch_size = 100

# ----------- 블록 번호 입력받아 슬라이싱 -----------
block_num = int(input(f"\n몇 번째 100개 구간을 실행할까요? (예: 1 → 1~100, 2 → 101~200): ").strip())
start_idx = (block_num - 1) * batch_size
end_idx = min(start_idx + batch_size, len(df))

target_df = df.iloc[start_idx:end_idx].reset_index(drop=True)
print(f"\n[{start_idx+1}번 ~ {end_idx}번 영화까지 진행]")

results = []
file_num = block_num  # 각 블록별로 파일 번호를 다르게

for index, row in target_df.iterrows():
    release_date = row['release_date']
    title = row['title']
    start = row['start_date'].strftime('%Y-%m-%d')
    end = row['end_date'].strftime('%Y-%m-%d')

    print(f"[{start_idx + index + 1}/{len(df)}] 요청 중: {title} ({start} ~ {end})")

    params = {
        "engine": "google_trends",
        "q": title,
        "date": f"{start} {end}",
        "geo": "KR",
        "hl": "ko",
        "api_key": API_KEY
    }
    try:
        search = GoogleSearch(params)
        data = search.get_dict()
        interest = data.get("interest_over_time", {})
        timeline_data = interest.get("timeline_data", [])

        day_values = [None] * 29
        if isinstance(timeline_data, list) and len(timeline_data) == 29:
            for i, entry in enumerate(timeline_data):
                values = entry.get("values", [])
                extracted_value = values[0].get("extracted_value", 0) if values else 0
                day_values[i] = extracted_value
        else:
            print(f"[{title}] → 검색 결과 없음 또는 날짜 수 불일치: {len(timeline_data)}개")

        row_result = {
            'title': title,
            'date': release_date.strftime('%Y-%m-%d'),
        }
        for i, v in enumerate(day_values):
            key = f'd{(i - 14) if (i - 14) < 0 else ("+" + str(i - 14)) if (i - 14) > 0 else ""}'
            row_result[key] = v if v is not None else 0

        results.append(row_result)
        time.sleep(1)

        # 매 요청 후 저장 (중간에 꺼져도 지금까지는 저장됨)
        out_file = f"trend_results_wide_part{file_num}.csv"
        with open(out_file, "w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=outcols)
            writer.writeheader()
            for r in results:
                writer.writerow({col: r.get(col, 0) for col in outcols})

    except Exception as e:
        print(f"에러 발생 - {title}: {e}")
        continue

print(f"\n완료! trend_results_wide_part{file_num}.csv로 저장되었습니다.")