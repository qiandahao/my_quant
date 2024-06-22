import requests
from requests.exceptions import ConnectionError
import time
import json
import pandas as pd
import os.path
from datetime import datetime
import os
import threading
from clickhouse_driver import Client

def merge_json(json1, json2):
    if isinstance(json1, list) and isinstance(json2, list):
        return json1 + json2
    elif isinstance(json1, dict) and isinstance(json2, dict):
        result = {}
        keys = set(json1.keys()).union(json2.keys())
        for key in keys:
            if key in json1 and key in json2:
                result[key] = merge_json(json1[key], json2[key])
            elif key in json1:
                result[key] = json1[key]
            else:
                result[key] = json2[key]
        return result
    else:
        return json1  # 如果不是列表或字典，则直接返回第一个值
 
def get_stock_us_list():
    url = 'https://stock.xueqiu.com/v5/stock/screener/quote/list.json?size=100&order=asc&order_by=symbol&market=US&type=us'
    url2 = 'https://stock.xueqiu.com/v5/stock/screener/quote/list.json?size=100&order=desc&order_by=symbol&market=US&type=us'
    
    headers = {
        'Cookie': 'device_id=e8a11bc724b7115f447ab4c08ef5beac; cookiesu=931702263491225; s=bg11zw7opj; snbim_minify=true; bid=aa3d1df992f90ca8cdd6773895b2f006_lus3wyz6; u=931702263491225; Hm_lvt_1db88642e346389874251b5a1eded6e3=1716359813; xq_a_token=0518d12486f7876b2f98097d9ec9214afa97c2a0; xqat=0518d12486f7876b2f98097d9ec9214afa97c2a0; xq_r_token=fc7d679707be09bbf6da361632fe9a5facb99f94; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOi0xLCJpc3MiOiJ1YyIsImV4cCI6MTcyMDE0MDcyMCwiY3RtIjoxNzE3NTgxMTk3MjQ0LCJjaWQiOiJkOWQwbjRBWnVwIn0.As2y5dZ9ZioN0J9VxW2FNkS7eiPPiDzMUk2KxXVPrHJV-14x7SXbxtU0JPnxMEWBEdZa6L_6pL-Ek4GRfC24KBlyiJ4MKsJm5E0wfTYXtGlEodH7wyPbitFWoEFek__w7DUESeoSzONr9ELWEX38Pw15XWu8rsXW7Uzd83w1I9WDuyLItrEdomNi_uSVyBNmOMS605q-OVOymoWXP2HHafdloT6pu2JXhYfoPrFb2RKZ6YvPXEG1O-CX-8nIEQUs2BqETwy80zItiFUe_AAcrSQFWVzuHb5GtTpdqFQE2aQze2beMNbIpYFXy0Hsqy7qHGV4eT2GpInnGH2yyZXoNw; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1717590253',
        'DNT': '1',
        'Sec-Fetch-Dest': 'document',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'sec-ch-ua-platform': 'macOS',
    }
    response = requests.get(url, headers=headers)
    result = ''

    formatted_date = "list"
    folder_name = "data/"

    if os.path.exists(folder_name + formatted_date + '.json'):
        print("list文件已经存在，跳过...")
        return
    index = 1
    while index < 100:
        print(index)
        temp = url + '&page=' + str(index)
        temp2 = url2 + '&page=' + str(index)
        try:
            response1 = requests.get(temp, headers=headers)
            index = index + 1

            response2 = requests.get(temp2, headers=headers)

            if response1.status_code == 200 and response2.status_code == 200:
                #json_string = json.dumps(response.json(), indent=4)
                
                    part1 = response1.json()
                    part2 = response2.json()
                    res = merge_json(part1, part2)
                    if result == '':
                        result = res
                    else:
                        result = merge_json(result, res)
            else:
                print("请求失败")
        except ConnectionError as e:
            # 捕获连接错误并打印错误信息
            print("连接错误:", e)
            # 等待一段时间后重试
            time.sleep(1)  # 可以根据需要调整等待时间
    now = datetime.now()
    timestamp_ms = str(now.timestamp() * 1000)
    # 格式化输出
    
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        print(f"文件夹 '{folder_name}' 创建成功")
    else:
        print(f"文件夹 '{folder_name}' 已经存在")
    json_string = json.dumps(result, indent=4)
    with open(folder_name + formatted_date + '.json', 'w') as file:
        file.write(json_string)

def create_files(start_index, end_index):
    for i in range(start_index, end_index):
        file_name = f"file_{i}.txt"
        with open(file_name, 'w') as file:
            file.write("Hello, World!")

def get_stock_us_data():
    now = datetime.now()
    timestamp_ms = str(int(now.timestamp() * 1000))
    print(timestamp_ms)
    # 格式化输出
    formatted_date = "list"
    folder_name = "data/"
    with open(folder_name + formatted_date + '.json', 'r') as file:
        data = json.load(file)
    symbols = [item['symbol'] for item in data['data']['list']]

    # 将数据转换为 DataFrame
    df = pd.DataFrame({'symbol': symbols})
    while True:
        try:
            for index, row in df.iterrows():
                if row['symbol'].find('-') != -1:
                    continue
                elif row['symbol'].find('.') != -1:
                    continue
                print("Symbol:", row['symbol'], " Start")

                file_name = row['symbol'] + '.json'
                if os.path.exists(folder_name + file_name):
                    print("JSON 文件已经存在，跳过...")
                else:
                    url = "https://stock.xueqiu.com/v5/stock/chart/kline.json?symbol="+ row['symbol'] + "&begin=" + timestamp_ms + "&period=day&type=before&count=-1&indicator=kline,pe,pb,ps,pcf,market_capital,agt,ggt,balance"
                    print(url)
                    # 发送GET请求获取网页内容
                    headers = {
                        'Cookie': 'cookiesu=931702263491225; s=bg11zw7opj; snbim_minify=true; bid=aa3d1df992f90ca8cdd6773895b2f006_lus3wyz6; u=931702263491225; Hm_lvt_1db88642e346389874251b5a1eded6e3=1716359813; device_id=1fa1304aabcac0d1a5db98dfddfee983; xq_a_token=483932c5fb313ca4c93e7165a31f179fb71e1804; xqat=483932c5fb313ca4c93e7165a31f179fb71e1804; xq_r_token=f3a274495a9b8053787677ff7ed85d1639c6e3e0; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOi0xLCJpc3MiOiJ1YyIsImV4cCI6MTcyMTQzNjcyOSwiY3RtIjoxNzE4ODQ3MTkzNDczLCJjaWQiOiJkOWQwbjRBWnVwIn0.qh129FV_Bo8_33CthG-kAjewrfCyPxvgltfbnn-yfTXygxQqlT1lGfeAAZta0IrF-OYAhA1eWgxuwhRSUN_Got2rdESYk2tLIpLIZ-yP3SrYYwYozCaXepFM4y8n1y8lkg45ng846NMvwCa1oSQj0Mjj8Y72HqHP146Fod1zwlxiMb0PAeIylLoe4XKQegjNP7uZWVVjnwd275y14HPsyQCDq_8wNGqV_RAOO8gf9SmIjUAFkdDIMO3nZzqNh9Zr9zlQKJORzbSYir-vRY6YsUKJ4qaCnE1IW9ru2cfBRI_FuozoXH9eeytfy3avfLYy_IV1dvk3JRGDT46nfukvDQ; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1718905782; is_overseas=0',
                        'DNT': '1',
                        'Sec-Fetch-Dest': 'document',
                        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                        'sec-ch-ua-platform': 'macOS',
                    }
                    response = requests.get(url, headers=headers)

                    # 检查请求是否成功
                    if response.status_code == 200:
                        json_string = json.dumps(response.json(), indent=4)
                        with open(folder_name + file_name, 'w') as file:
                            file.write(json_string)
                        print("Symbol:", row['symbol'], " Done")
                    else:
                        print("请求失败")
                    time.sleep(0.1)
            return
        except ConnectionError as e:
            # 捕获连接错误并打印错误信息
            print("连接错误:", e)
            # 等待一段时间后重试
            time.sleep(5)  # 可以根据需要调整等待时间

def generate_stock_csv():
    with open('data/list.json', 'r') as file:
        data = json.load(file)
    symbols = [item['symbol'] for item in data['data']['list']]
    df = pd.DataFrame({'symbol': symbols})
    folder_name = 'csv/'
    # 将数据转换为 DataFrame
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)

    for index, row in df.iterrows():
        if not os.path.exists('data/'+ row['symbol'] + '.json'):
            continue
        json_data = pd.read_json('data/'+ row['symbol'] + '.json')
        
        df = pd.DataFrame(json_data['data']['item'], columns=json_data['data']['column'])
        df['symbol'] = row['symbol']
        # 将DataFrame保存为CSV文件
        csv_file = folder_name + row['symbol'] + '.csv'
        df.to_csv(csv_file, index=False)

#get_stock_us_list()
#get_stock_us_data()
start_time = time.time()
get_stock_us_list()
threads = []
for _ in range(10):
    thread = threading.Thread(target=get_stock_us_data)
    threads.append(thread)
    thread.start()

# 等待所有线程退出
for thread in threads:
    thread.join()
generate_stock_csv()
    # 从 JSON 文件加载数据到 DataFrame
# 记录结束时间
end_time = time.time()

# 计算并打印运行时间
elapsed_time = end_time - start_time
print(f"All threads have exited. Elapsed time: {elapsed_time} seconds")
    # 从 JSON 文件加载数据到 DataFrame
