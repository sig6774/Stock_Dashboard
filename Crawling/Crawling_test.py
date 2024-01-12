import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd

'''
함수 목적 : 특정 주식 종목의 title 및 url, datetime 크롤링 
Input : 주식 종목 번호 
Output : Dataframe(title, url, datetime)
'''
def crawl_naver_stock_news(stock_code, days=7):
    # 네이버 주식 뉴스 URL 설정
    base_url = f"https://finance.naver.com/item/news_news.naver?code={stock_code}&page="

    # 결과를 저장할 리스트
    news_list = []

    # 지난 일주일간의 뉴스를 가져오기 위한 날짜 설정
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    for page in range(1, 10):  # 예시로 10페이지까지만 확인
        # 페이지별 URL 구성
        url = base_url + str(page)
        
        # 페이지 접속할 때 동작이 원활하지 못하면 종료 
        response = requests.get(url)
        if response.status_code != 200:
            break
        
        # Html parsing 
        soup = BeautifulSoup(response.text, 'html.parser')

        # 뉴스 목록 추출
        news_table = soup.find('table', class_='type5')
        if not news_table:
            break

        for tr in news_table.find_all('tr'):
            # 광고 등 필요 없는 행은 제외
            if 'class' in tr.attrs:  
                continue
            
            # tag와 class는 변동 가능성 존재
            # 뉴스 제목과 링크 추출
            a_tag = tr.find('a', class_='tit')
            if None is not a_tag:
                title = a_tag.text
                link = "https://finance.naver.com" + a_tag['href']

            date_str = tr.find('td', class_="date")
            if None is not date_str:
                # 날짜 추출 및 형식 변환
                date_str = date_str.text.lstrip()
                news_date = datetime.strptime(date_str, "%Y.%m.%d %H:%M")

                # 지정된 기간 내의 뉴스만 선택
                if start_date <= news_date <= end_date:
                    news_list.append([title, news_date, link])
    # print(new_list)    
    # 결과를 DataFrame으로 변환
    df = pd.DataFrame(news_list, columns=['Title', 'Date', 'Link'])
    return df

df = crawl_naver_stock_news('005930')
df.head()