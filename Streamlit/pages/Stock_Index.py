import sys
import db_info
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import DB_Management
import psycopg2
sys.path.append("/home/sig/Data-Pipeline/Code/Streamlit/")


st.set_page_config(
    page_title="Stock_Index"
)

st.title("Check Stock Index Information")

# db 정보
db_config = {
    'host': db_info.host,
    'database': db_info.database,
    'user': db_info.user,
    'password': db_info.password,
    'port': db_info.port
}

# 데이터베이스 연결
conn = psycopg2.connect(**db_config)

# 커서 생성
cur = conn.cursor()

# 현재 날짜 및 시간 가져오기
now = datetime.now()

# 예외처리를 위한 오늘 날짜 변수
today_date = now.strftime("%Y-%m-%d")

# 하루 전 날짜 계산
yesterday = now - timedelta(days=1)

# 날짜를 YYYY-MM-DD 포맷으로 변환
yesterday_date = yesterday.strftime("%Y-%m-%d")
# 하루 전 날짜를 계산한 이유는 오늘 날짜에 대한 지수 데이터는 조회할 수 없으므로 하루 전 날짜 추출


def draw_chart(df):

    # 날짜를 datetime으로 변환하고 인덱스로 설정
    df['Date'] = pd.to_datetime(df['date']).dt.date
    df.set_index('Date', inplace=True)

    # 2개의 열 생성
    col1, col2 = st.columns(2)

    # 첫 번째 열에 주가 선 차트 그리기
    with col1:
        st.line_chart(df['close'])

    # 두 번째 열에 볼륨 막대 차트 그리기
    with col2:
        st.line_chart(df['volume'])


index_list_db, origin_index_list = DB_Management.search_index_name_list(cur)
# print(origin_index_list)

# index 조회 가능하도록 하고 아래에 볼륨도 볼 수 있게 구현

stock_name = st.selectbox(
    'Choose an option',
    ([index_list_db[i] for i in range(len(index_list_db))]))


start_date = str(st.date_input('Start date'))

if start_date != today_date:

    for_search_index_name = stock_name+"_df"

    index_df = DB_Management.search_index(
        cur, for_search_index_name, start_date, yesterday_date
        )
    draw_chart(index_df)
