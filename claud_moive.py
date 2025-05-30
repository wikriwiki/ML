import requests
import pandas as pd
import time
from datetime import datetime

# 🔐 KOFIC API 키
API_KEY = "your_key"

# 📊 기존 박스오피스 CSV 파일 읽기
print("📂 기존 박스오피스 데이터 로딩...")
try:
    df_boxoffice = pd.read_csv("kobis_box_office_sorted_2019_2020.csv") # 각자의 원천 파일 경로로 변경경
    print(f"✅ 박스오피스 데이터 로딩 완료: {len(df_boxoffice)}개 레코드")
except FileNotFoundError:
    print("❌ kobis_box_office_sorted_2019_2020.csv 파일을 찾을 수 없습니다.")
    exit()

# 🎬 중복 제거된 영화 코드 추출
unique_movie_codes = set(df_boxoffice['movieCd'].dropna().astype(str))
print(f"🎯 중복 제거된 영화 수: {len(unique_movie_codes)}개")

# 🔍 영화 상세정보 수집 함수
def get_movie_info(movie_cd):
    """영화 코드로 영화 상세정보 조회"""
    url = f"http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"
    params = {
        'key': API_KEY,
        'movieCd': movie_cd
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        movie_info_result = data.get('movieInfoResult', {})
        movie_info = movie_info_result.get('movieInfo', {})
        
        if not movie_info:
            return None
            
        # 영화 상세정보 추출
        details = {
            'movieCd': movie_info.get('movieCd', ''),
            'movieNm': movie_info.get('movieNm', ''),
            'movieNmEn': movie_info.get('movieNmEn', ''),
            'movieNmOg': movie_info.get('movieNmOg', ''),
            'showTm': movie_info.get('showTm', ''),
            'prdtYear': movie_info.get('prdtYear', ''),
            'openDt': movie_info.get('openDt', ''),
            'prdtStatNm': movie_info.get('prdtStatNm', ''),
            'typeNm': movie_info.get('typeNm', ''),
            'repNationNm': movie_info.get('repNationNm', ''),
            'repGenreNm': movie_info.get('repGenreNm', ''),
            'watchGradeNm': movie_info.get('watchGradeNm', '')
        }
        
        # 장르 정보 (여러 개일 수 있음)
        genres = movie_info.get('genres', [])
        genre_list = [genre.get('genreNm', '') for genre in genres]
        details['genres'] = '|'.join(genre_list)
        
        # 국가 정보 (여러 개일 수 있음)
        nations = movie_info.get('nations', [])
        nation_list = [nation.get('nationNm', '') for nation in nations]
        details['nations'] = '|'.join(nation_list)
        
        # 감독 정보 (여러 명일 수 있음)
        directors = movie_info.get('directors', [])
        director_list = [director.get('peopleNm', '') for director in directors]
        details['directors'] = '|'.join(director_list)
        
        # 배우 정보 (주요 배우만)
        actors = movie_info.get('actors', [])
        actor_list = [actor.get('peopleNm', '') for actor in actors[:5]]  # 상위 5명만
        details['actors'] = '|'.join(actor_list)
        
        # 제작사 정보
        companys = movie_info.get('companys', [])
        company_list = [company.get('companyNm', '') for company in companys if company.get('companyPartNm') == '제작사']
        details['productionCompanies'] = '|'.join(company_list)
        
        # 배급사 정보
        distributor_list = [company.get('companyNm', '') for company in companys if company.get('companyPartNm') == '배급사']
        details['distributors'] = '|'.join(distributor_list)
        
        return details
        
    except Exception as e:
        print(f"⚠️ 영화 {movie_cd} 정보 수집 오류: {e}")
        return None

# 📋 기존에 처리된 영화 확인
print("🔍 기존 처리된 영화 확인...")
existing_movie_details = []
try:
    df_existing = pd.read_csv("kobis_movie_details_2019_2020.csv")
    existing_movie_codes = set(df_existing['movieCd'].astype(str))
    existing_movie_details = df_existing.to_dict('records')
    print(f"✅ 기존 처리된 영화: {len(existing_movie_codes)}개")
except FileNotFoundError:
    existing_movie_codes = set()
    print("📝 기존 파일 없음 - 처음부터 시작")

# 아직 처리되지 않은 영화 코드만 추출
remaining_movie_codes = unique_movie_codes - existing_movie_codes
print(f"🎯 처리해야 할 남은 영화: {len(remaining_movie_codes)}개")

if len(remaining_movie_codes) == 0:
    print("✅ 모든 영화가 이미 처리되었습니다!")
    movie_details = existing_movie_details
else:
    # 📋 영화 상세정보 수집 (남은 것만)
    print("🔍 남은 영화 상세정보 수집 시작...")
    movie_details = existing_movie_details.copy()  # 기존 데이터 포함
    processed_count = len(existing_movie_codes)  # 이미 처리된 개수
    total_count = len(unique_movie_codes)

    for movie_cd in remaining_movie_codes:
        processed_count += 1
        print(f"진행률: {processed_count}/{total_count} ({processed_count/total_count*100:.1f}%) - 처리중: {movie_cd}")
        
        movie_info = get_movie_info(movie_cd)
        if movie_info:
            movie_details.append(movie_info)
            print(f"✅ 성공: {movie_info.get('movieNm', 'Unknown')}")
        else:
            print(f"❌ 실패: {movie_cd}")
        
        # API 호출 제한을 위한 대기
        time.sleep(0.3)
        
        # 진행상황 중간 저장 (50개마다)
        if (processed_count - len(existing_movie_codes)) % 50 == 0:
            temp_df = pd.DataFrame(movie_details)
            temp_df.to_csv(f"temp_movie_details_{processed_count}.csv", index=False, encoding='utf-8-sig')
            print(f"💾 중간 저장 완료: {processed_count}개 처리")

print(f"🎉 영화 상세정보 수집 완료: {len(movie_details)}개")

# 📊 영화 상세정보 데이터프레임 생성
df_movie_details = pd.DataFrame(movie_details)

# 💾 독립적인 영화 상세정보 CSV 저장
details_save_path = "kobis_movie_details_2019_2020.csv" # 각자의 연도에 맞게 변경
df_movie_details.to_csv(details_save_path, index=False, encoding='utf-8-sig')
print(f"✅ 영화 상세정보 CSV 저장 완료: {details_save_path}")

# 🔗 박스오피스 데이터와 영화 상세정보 병합
print("🔗 데이터 병합 시작...")

# 데이터 타입 확인 및 통일
print(f"📊 박스오피스 movieCd 타입: {df_boxoffice['movieCd'].dtype}")
print(f"📊 영화상세정보 movieCd 타입: {df_movie_details['movieCd'].dtype}")

# movieCd를 문자열로 통일
df_boxoffice['movieCd'] = df_boxoffice['movieCd'].astype(str)
df_movie_details['movieCd'] = df_movie_details['movieCd'].astype(str)

print("✅ movieCd 데이터 타입 통일 완료")

df_merged = df_boxoffice.merge(
    df_movie_details, 
    on='movieCd', 
    how='left',
    suffixes=('_box', '_detail')
)

print(f"📊 병합 결과: {len(df_merged)}개 레코드")

# 🔍 병합 통계 확인
merged_with_details = df_merged[df_merged['movieNm_detail'].notna()]
print(f"📈 상세정보가 있는 레코드: {len(merged_with_details)}개")
print(f"📉 상세정보가 없는 레코드: {len(df_merged) - len(merged_with_details)}개")

# 💾 병합된 데이터 저장
merged_save_path = "kobis_boxoffice_with_details_2019_2020.csv"
df_merged.to_csv(merged_save_path, index=False, encoding='utf-8-sig')
print(f"병합된 데이터 CSV 저장 완료: {merged_save_path}")

# 📋 최종 결과 요약
print("\n" + "="*50)
print("📊 최종 수집 결과 요약")
print("="*50)
print(f"🎬 총 영화 수: {len(unique_movie_codes)}개")
print(f"✅ 상세정보 수집 성공: {len(movie_details)}개")
print(f"❌ 상세정보 수집 실패: {len(unique_movie_codes) - len(movie_details)}개")
print(f"📊 최종 병합 레코드: {len(df_merged)}개")
print(f"💾 저장된 파일:")
print(f"   - 영화 상세정보: {details_save_path}")
print(f"   - 병합된 데이터: {merged_save_path}")

import glob
import os
temp_files = glob.glob("temp_movie_details_*.csv")
if temp_files:
    print(f"\n🧹 임시 파일 {len(temp_files)}개 발견")
    cleanup = input("임시 파일을 삭제하시겠습니까? (y/n): ")
    if cleanup.lower() == 'y':
        for temp_file in temp_files:
            os.remove(temp_file)
        print("✅ 임시 파일 정리 완료")

print("\n🎉 모든 작업이 완료되었습니다!")
