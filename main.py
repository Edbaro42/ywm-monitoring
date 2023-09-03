import requests
import mariadb
import pandas as pd
from datetime import datetime
import pytz
import sys

timezone = pytz.timezone("Europe/Moscow")
current_time = datetime.now(timezone)

# Параметры API и базы данных
api_url = 'https://api.webmaster.yandex.net/v4/user/123321123/hosts/https:site.ru:443/query-analytics/list'
headers = {'Authorization': 'OAuth YOUR_OAUTH_TOKEN'}
db_host = 'localhost'
db_port = 3306
db_user = 'your_username'
db_password = 'your_password'
db_name = 'your_database'
table_name = 'your_table'

# Функция для выполнения запроса к API
def api_request(url, headers, body):
    response = requests.post(url, headers=headers, json=body)
    if response.status_code != 200:
        print(f"{current_time} - Код ответа сервера API: {response.status_code}")
        sys.exit()
    return response.json()

# Функция для создания таблицы, если она не существует
def create_table(cursor):
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ("
                   "ID INT AUTO_INCREMENT PRIMARY KEY,"
                   "DATE DATE,"
                   "QUERY VARCHAR(100),"
                   "POSITION DECIMAL(14, 2),"
                   "DEMAND DECIMAL(14, 2),"
                   "IMPRESSIONS DECIMAL(14, 2),"
                   "CLICKS DECIMAL(14, 2),"
                   "CTR DECIMAL(14, 2),"
                   "CONSTRAINT unique_date_query UNIQUE (DATE, QUERY)"
                   ")")

# Функция для вставки данных в таблицу
def insert_data(cursor, data):
    temp_data = []
    
    for item in data['text_indicator_to_statistics']:
        QUERY = item['text_indicator']['value']
        for stat in item['statistics']:
            DATE = stat['date']
            DEMAND = 0.0
            CLICKS = 0.0
            CTR = 0.0
            IMPRESSIONS = 0.0
            POSITION = 0.0
            
            if stat['field'] == 'DEMAND':
                DEMAND = round(float(stat['value']), 2)
            elif stat['field'] == 'CLICKS':
                CLICKS = round(float(stat['value']), 2)
            elif stat['field'] == 'CTR':
                CTR = round(float(stat['value']), 2)
            elif stat['field'] == 'IMPRESSIONS':
                IMPRESSIONS = round(float(stat['value']), 2)
            elif stat['field'] == 'POSITION':
                POSITION = round(float(stat['value']), 2)
            
            temp_data.append({
                'DATE': DATE,
                'QUERY': QUERY,
                'POSITION': POSITION,
                'DEMAND': DEMAND,
                'IMPRESSIONS': IMPRESSIONS,
                'CLICKS': CLICKS,
                'CTR': CTR
            })
    
    temp_data = pd.DataFrame(temp_data)
    aggregated_data = temp_data.groupby(['DATE', 'QUERY']).sum().reset_index()
    return aggregated_data

# Подключение к базе данных
try:
    conn = mariadb.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name
    )
    cursor = conn.cursor()

    # Проверка наличия и создание таблицы
    create_table(cursor)

    offset = 0

    while True:
        # Выполнение запроса к API
        request_body = {
            "offset": offset,
            "limit": 100,
            "device_type_indicator": "ALL",
            "text_indicator": "QUERY",
            "region_ids": [225],
            "filters": {
                "text_filters": [
                    {"text_indicator": "QUERY", "operation": "TEXT_CONTAINS", "value": ""}
                ]
            }
        }
        api_response = api_request(api_url, headers, request_body)

        # Вставка данных в таблицу и агрегирование с использованием pandas
        aggregated_data = insert_data(cursor, api_response)

        # Запись данных в таблицу
        query = f"INSERT IGNORE INTO {table_name} (DATE, QUERY, POSITION, DEMAND, IMPRESSIONS, CLICKS, CTR) " \
                f"VALUES (%s, %s, %s, %s, %s, %s, %s)"
        data_to_insert = [(row.DATE, row.QUERY, row.POSITION, row.DEMAND, row.IMPRESSIONS, row.CLICKS, row.CTR)
                          for row in aggregated_data.itertuples()]
        cursor.executemany(query, data_to_insert)

        if offset - (api_response['count'] - (api_response['count'] % 100)) < 0:
        # Увеличение значения offset на 100 или 1
            offset += 100
        else:
            offset += 1

        # Проверка условия выхода из цикла
        if offset == api_response['count'] - 1:
            break

        # Сохранение изменений и закрытие соединения
        conn.commit()
        
    conn.close()
    
    print(f"{current_time} - Успешно!")
except mariadb.Error as e:
    print(f"{current_time} - Ошибка при работе с базой данных: {e}")
