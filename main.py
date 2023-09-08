import requests
import mariadb
import pandas as pd
from datetime import datetime
import pytz
import sys

timezone = pytz.timezone("Europe/Moscow")
current_time = datetime.now(timezone)

api_url = 'https://api.webmaster.yandex.net/v4/user/123321123/hosts/https:site.ru:443/query-analytics/list'
headers = {'Authorization': 'OAuth YOUR_OAUTH_TOKEN'}
db_host = 'localhost'
db_port = 3306
db_user = 'your_username'
db_password = 'your_password'
db_name = 'your_database'
table_name = 'aggregate'
region_id_var = 11079 # 225 - РФ, 1 - Мск и МО, 10174 - Спб и ЛО, 11079 - НН и НО
device_type_indicator_var = "ALL" # "DESKTOP", "MOBILE_AND_TABLET", "MOBILE"

def api_request(url, headers, body):
    response = requests.post(url, headers=headers, json=body)
    if response.status_code != 200:
        print(f"{current_time} - {sys.argv[0]} - Ответ API: {response.status_code}")
        sys.exit()
    return response.json()

def create_table(cursor):
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ("
                   "URL VARCHAR(255),"
                   "DATE DATE,"
                   "QUERY VARCHAR(100),"
                   "POSITION DECIMAL(14, 2),"
                   "DEMAND DECIMAL(14, 2),"
                   "IMPRESSIONS DECIMAL(14, 2),"
                   "CLICKS DECIMAL(14, 2),"
                   "CTR DECIMAL(14, 2),"
                   "CONSTRAINT unique_url_date_query UNIQUE (URL, DATE, QUERY)"
                   ")")

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
                'URL': add_url,
                'DATE': DATE,
                'QUERY': QUERY,
                'POSITION': POSITION,
                'DEMAND': DEMAND,
                'IMPRESSIONS': IMPRESSIONS,
                'CLICKS': CLICKS,
                'CTR': CTR
            })
    temp_data = pd.DataFrame(temp_data)
    aggregated_data = temp_data.groupby(['URL', 'DATE', 'QUERY']).sum().reset_index()
	
    # Замена нулевых значений POSITION при условии, что DEMAND больше нуля
    condition = (aggregated_data['DEMAND'] > 0)
    aggregated_data.loc[condition, 'POSITION'] = aggregated_data['POSITION'].replace(0.00, 101.00)
	
    return aggregated_data

try:
    conn = mariadb.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name
    )
    cursor = conn.cursor()
    create_table(cursor)

    offset_url = 0
    limit_url = 100
    url_list = []
    
    # Получаем список URL
    while True:
        request_url_body = {
            "offset": offset_url,
            "limit": limit_url,
            "device_type_indicator": device_type_indicator_var,
            "text_indicator": "URL",
            "region_ids": [region_id_var],
            "filters": {
                "text_filters": [
                    {"text_indicator": "URL", "operation": "TEXT_CONTAINS", "value": ""}
                ]
            }
        }

        api_url_response = api_request(api_url, headers, request_url_body)
        url_list.extend([item['text_indicator']['value'] for item in api_url_response['text_indicator_to_statistics']])
        
        if offset_url - (api_url_response['count'] - (api_url_response['count'] % 100)) < 0 and api_url_response['count'] > 99:
            limit_url = 100
            offset_url += 100
        else:	
            break
        
        if offset_url >= api_url_response['count'] - 1:
            break

    for url in url_list:
        add_url = url
        offset_query = 0
        limit_query = 100
        query_data_list = []
        
        # Получаем данные QUERY для каждого URL 
        while True:
            request_query_body = {
                "offset": offset_query,
                "limit": limit_query,
                "device_type_indicator": device_type_indicator_var,
                "text_indicator": "QUERY",
                "region_ids": [region_id_var],
                "filters": {
                    "text_filters": [
                        {"text_indicator": "URL", "operation": "TEXT_MATCH", "value": url}
                    ]
                }
            }
            
            api_query_response = api_request(api_url, headers, request_query_body)
            query_data_list.extend(api_query_response['text_indicator_to_statistics'])
            
            if offset_query - (api_query_response['count'] - (api_query_response['count'] % 100)) < 0 and api_query_response['count'] > 99:
                limit_query = 100
                offset_query += 100
            else:	
                break
            
            if offset_query >= api_query_response['count'] - 1:
                break
        		
        # Агрегируем данные и записываем в БД
        query_data = insert_data(cursor, {'text_indicator_to_statistics': query_data_list})
        data_to_insert = [(row.URL, row.DATE, row.QUERY, row.POSITION, row.DEMAND, row.IMPRESSIONS, row.CLICKS, row.CTR)
                          for row in query_data.itertuples()]
        query = f"INSERT IGNORE INTO {table_name} (URL, DATE, QUERY, POSITION, DEMAND, IMPRESSIONS, CLICKS, CTR) " \
                f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.executemany(query, data_to_insert)
        conn.commit()
    
    conn.close()
    print(f"{current_time} - {sys.argv[0]} - Успешно!")

except mariadb.Error as e:
    print(f"{current_time} - {sys.argv[0]} - Ошибка базы данных: {e}")
