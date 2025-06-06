import os
import sys
import urllib.request
import pandas as pd
from datetime import datetime, timedelta
import time
import json

client_id = "네이버 데이터랩에서 발급받은 id"
client_secret = "네이버 데이터랩에서 발급받은 pw"
url = "https://openapi.naver.com/v1/datalab/search"

# 경로 수정해주셔야합니다
df = pd.read_csv('naver_trend_prepare.csv')

results = []

genders = ['m', 'f']
ages_list = ['2', '3', '4', '5', '6', '7', '8', '9', '10', '11']

# tail안의 숫자로 뒤에서부터 api호출 
for idx, row in df.tail(5).iterrows():  # ★여기서 영화 5개로 제한
    keywords = [str(row['movieNm_detail']).strip()]
    if pd.notna(row.get('movieNmEn', '')) and str(row['movieNmEn']).strip():
        keywords.append(str(row['movieNmEn']).strip())
    if pd.notna(row.get('movieNmOg', '')) and str(row['movieNmOg']).strip():
        keywords.append(str(row['movieNmOg']).strip())
    

    try:
        open_dt = datetime.strptime(str(row['openDt_box']), '%Y-%m-%d')
    except Exception:
        continue
    # 2016년 1월 이후로부터 검색가능
    if open_dt <= datetime(2016, 2, 1): 
        continue
# 개봉일 기준 30일 전부터 30일 후까지 총 60일
# 추가 필요 시 개봉한 달로 변경 가능
    start_date = (open_dt - timedelta(days=30)).strftime('%Y-%m-%d')
    end_date = (open_dt + timedelta(days=30)).strftime('%Y-%m-%d')

    for gender in genders:    
        for age in ages_list: 
            print(f"Requesting {row['movieNm_detail']} | gender={gender} | age={age}")  # 진행상황 표시

            body = {
                "startDate": str(start_date),
                "endDate": str(end_date),
                # timeUnit 값을 day, week, month중 택 1
                "timeUnit": "week",
                "keywordGroups": [
                    {
                        "groupName": str(row['movieNm_detail']),
                        "keywords": keywords
                    }
                ],
                "gender": gender,
                "ages": [age]
            }

            body_json = json.dumps(body, ensure_ascii=False).encode('utf-8')

            request = urllib.request.Request(url)
            request.add_header("X-Naver-Client-Id", client_id)
            request.add_header("X-Naver-Client-Secret", client_secret)
            request.add_header("Content-Type", "application/json")

            try:
                response = urllib.request.urlopen(request, data=body_json, timeout=10)  # ★timeout=10초 추가
                rescode = response.getcode()
                if rescode == 200:
                    response_body = response.read().decode('utf-8')
                    results.append({
                        "movieNm_detail": row['movieNm_detail'],
                        "gender": gender,
                        "age": age,
                        "request_body": body,
                        "response": response_body
                    })
                else:
                    print("Error Code:", rescode)
                    print("Request Body:", body)
                    print(response.read().decode('utf-8'))
            except Exception as e:
                print("Request failed:", e)
                print("Request Body:", body)

            time.sleep(1)

# 결과를 DataFrame으로 변환 및 CSV 저장
results_df = pd.DataFrame(results)
results_df.to_csv('naver_trend_result_sample_gender_ages_week.csv', index=False, encoding='utf-8-sig')
print('CSV 저장 완료: naver_trend_result_sample_gender_ages_day.csv')