import streamlit as st
import FinanceDataReader as fdr
from datetime import datetime
import DB_Management

st.set_page_config(page_title="main", initial_sidebar_state="collapsed")

# stock name과 초기값 (sql)
stock_name_list, first_value = DB_Management.get_name_query()

# 둘 중 하나의 값만 입력되도록 하는 로직
col1, col2 = st.columns(2)

# 변수 초기화
user_input_text = None
stock_name = None

# 원하는 주식의 번호 입력
with col1:
    user_input_text = st.text_input('Write Stock number')

# 주식 번호를 모르겠다면 주식 이름을 보여줄 수 있는 select box
with col2:
    stock_name = st.selectbox(
        'Choose an option',
        ([stock_name_list[i][0] for i in range(len(stock_name_list))]))

# Error Case 대비 2
# 하나의 입력만 처리
if user_input_text:
    st.write(f"You entered stock number: {user_input_text}")
    stock_name = None  # 주식 이름 선택 무시
elif stock_name and stock_name != first_value:
    st.write(f"You selected stock name: {stock_name}")
    user_input_text = None  # 주식 번호 입력 무시

# Error Case 대비 3
# 주식 차트를 보여주기 위해서는 오늘 날짜는 제외해야함으로 비교 진행하기 위해 추가
# 시작 날짜 입력 받기
start_date = str(st.date_input('Start date'))

# 현재 날짜 및 시간 가져오기
now = datetime.now()

# 날짜를 YYYY-MM-DD 포맷으로 변환
today_date = now.strftime("%Y-%m-%d")


# stock_num 선언
if user_input_text is not None:
    stock_num = user_input_text
elif stock_name is not None and stock_name != first_value:
    # stock name으로 stock number 선언 (sql)
    stock_num = DB_Management.get_stock_code(stock_name)

# try except 구문 추가
if (stock_name is not None and today_date != start_date and
        stock_name != first_value):
    try:
        stock_dataframe = fdr.DataReader(stock_num[0][0], start_date)

        st.line_chart(stock_dataframe["Close"])
    except KeyError:
        st.write("에러가 발생했습니다. 다시 시도해주세요.")

elif (user_input_text is not None and today_date != start_date and
        stock_name != first_value):
    try:
        stock_dataframe = fdr.DataReader(user_input_text, start_date)

        st.line_chart(stock_dataframe["Close"])
    except KeyError:
        st.write("주식 고유번호가 잘못되었습니다. 다시 시도해주세요.")
