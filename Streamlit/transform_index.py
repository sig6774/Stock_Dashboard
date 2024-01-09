import re

# 한국 및 주요국가의 주가 지수 데이터 수집
index_list = [
    "KS11",
    "KQ11",
    "IXIC",
    "S&P500",
    "RUT",
    "VIX",
    "SSEC",
    "N225",
    "FTSE",
    "FCHI",
    "GDAXI"]


def transform_special_char(list_str: list):

    new_index_list = []

    for i in list_str:
        t_string = re.sub(r'[^\w\s]', '', i)
        new_index_list.append(t_string)

    return new_index_list