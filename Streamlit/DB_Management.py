import psycopg2
import db_info
from psycopg2 import sql
import pandas as pd

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


def create_stock_table(db_cursor, db_conn):
    query = sql.SQL("""
    CREATE TABLE IF NOT EXISTS finance.stocks (
        code VARCHAR(30) PRIMARY KEY,
        isu_cd VARCHAR(30),
        name VARCHAR(50),
        market VARCHAR(30))
    """)

    db_cursor.execute(query)
    db_conn.commit()
    print("Create Korean Stock Table Complete")


def create_index_table(db_cursor, db_conn, index_name):
    for i in range(len(index_name)):

        query = sql.SQL(f"""
        CREATE TABLE IF NOT EXISTS finance.{index_name[i]} (
            Date VARCHAR(30) PRIMARY KEY,
            Open VARCHAR(30),
            High VARCHAR(30),
            Low VARCHAR(30),
            Close VARCHAR(30),
            Volume VARCHAR(30))
        """)

        db_cursor.execute(query)
        db_conn.commit()
    print("Create Index Table Complete")


def get_name_query():
    # 데이터베이스에서 주식 이름 데이터를 가져올 SQL 쿼리 작성
    query = "SELECT name FROM finance.stocks"
    cur.execute(query)
    # Error Case 대비 1
    # 첫번째로 선택되는 값이 default로 선언되는 것을 방지하기 위해 맨 앞에 새로운 value 추가
    result_name = cur.fetchall()
    first_value = "Stock Name"
    result_name.insert(0, (first_value,))

    return result_name, first_value


def get_stock_code(stock_name: str):
    query = f"SELECT code FROM finance.stocks WHERE name='{stock_name}'"
    cur.execute(query)
    stock_num = cur.fetchall()

    return stock_num


def insert_stock_table(db_cursor, db_conn, dataframe):
    for i in range(len(dataframe)):
        query = "INSERT INTO finance.stocks VALUES ('%s', '%s', '%s', '%s') ON CONFLICT (CODE) DO NOTHING" \
            % (dataframe.loc[i]["Code"], dataframe.loc[i]["ISU_CD"],
                dataframe.loc[i]["Name"], dataframe.loc[i]["Market"])

        db_cursor.execute(query)
        db_conn.commit()
    print("Insert Complete")


def select_stock_table(db_cursor):
    query = "select * from finance.stocks limit 5;"
    db_cursor.execute(query)
    result_query = db_cursor.fetchall()

    return result_query


def drop_stock_table(db_cursor, db_conn):
    query = sql.SQL("""
    DROP TABLE finance.stocks
    """)

    db_cursor.execute(query)
    db_conn.commit()
    print("Delete Complete!")


# index name 조회
def search_index_name_list(db_cursor):

    index_list_query = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'finance';
    """

    try:
        db_cursor.execute(index_list_query)
        # 변환한 index이름과 추후 검색을 하기 위해 원본 index 정보를 넣을 빈 리스트 생성

        new_table_list = []
        origin_table_list = []

        table_list = db_cursor.fetchall()

        for i in range(len(table_list)):
            if "stocks" != table_list[i][0]:
                replace_index_name = table_list[i][0].replace("_df", "")
                new_table_list.append(replace_index_name)
                origin_table_list.append(table_list[i][0])

        return new_table_list, origin_table_list

    except Exception as e:
        print(f"Error occured : {e}")


# 특정 인덱스 날짜 조회
def search_index(db_cursor, index_name, start_date, end_date):

    index_search_query = f"""
        select date, close, volume
        from finance.{index_name}
        where to_date(date, 'YYYY-MM-DD')
        between '{start_date}' and '{end_date}'
    """

    try:
        db_cursor.execute(index_search_query)

        index_query = db_cursor.fetchall()

        # 컬럼명
        columns = [col[0] for col in db_cursor.description]  # 수정된 부분

        df = pd.DataFrame(index_query, columns=columns)

        # 'close'와 'volume' 열을 숫자로 변환
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')

        return df
    except Exception as e:
        print(f"Error occured : {e}")
