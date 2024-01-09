import psycopg2
import FinanceDataReader as fdr
import DB_Management
import transform_index

index_list = transform_index.index_list

db = DB_Management.db_config

# 데이터베이스 연결
conn = psycopg2.connect(**db)

# 커서 생성
cur = conn.cursor()

new_index_list = transform_index.transform_special_char(index_list)
# 주요 국가 Index 테이블 생성
DB_Management.create_index_table(cur, conn, new_index_list)

# 한국 거래소 상장 종목 테이블 생성
DB_Management.create_stock_table(cur, conn)

# 한국거래소 상장종목 전체
df_krx = fdr.StockListing('KRX')

# DB에 넣기 위해 컬럼 정제
extract_dataframe = df_krx[["Code", "ISU_CD", "Name", "Market"]]

DB_Management.insert_stock_table(cur, conn, extract_dataframe)

# DB_Management.select_stock_table(cur)
