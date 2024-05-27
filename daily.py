import requests
from requests.exceptions import ConnectionError
import time
import json
from bs4 import BeautifulSoup
import pandas as pd
import os.path
from datetime import datetime

def get_stock_us_list():
    url = 'https://stock.xueqiu.com/v5/stock/screener/quote/list.json?size=5000&order=desc&order_by=percent&market=US&type=us'
    headers = {
        'Cookie': 'device_id=e8a11bc724b7115f447ab4c08ef5beac; cookiesu=931702263491225; s=bg11zw7opj; __utma=1.1301827076.1710235788.1710235788.1710235788.1; __utmc=1; __utmz=1.1710235788.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); snbim_minify=true; smidV2=202403280849125de654b47643f6772e389fb8680f9a90000e32c4c27adc350; bid=aa3d1df992f90ca8cdd6773895b2f006_lus3wyz6; .thumbcache_f24b8bbe5a5934237bbc0eda20c1b6e7=p8It1Sxugp3IwvGKdOdGiotK7qnKgOccLoRu+Xp50bqyLzzUNN3C/MMnVnOYgTMcpdiRa5O/vqO9kIXKLJiK3g%3D%3D; acw_tc=276077a817163598111741134e6447a656c11bc217806699b22e5e4d5d4277; xq_a_token=c2aefa380b9072a563e961143570e259329d659f; xqat=c2aefa380b9072a563e961143570e259329d659f; xq_r_token=2823a23fcab28b5723fbd7c5220a4ba4cc755a52; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOi0xLCJpc3MiOiJ1YyIsImV4cCI6MTcxODg0NDcxMiwiY3RtIjoxNzE2MzU5NzU1MjU3LCJjaWQiOiJkOWQwbjRBWnVwIn0.VYFVyQFbyB3VYZqD2rSF5NHC8jErHCUTB5W59UetzZJ0128gPgfV7289QgUiOuoe5khAF5dOzY--abQFsEE-0IP_0yEfAI7G-qSUiWYBoyyy7yRgeh2x0imFsb8_lsBKItvQ1oFrar05CtlIOLUynWSaKoZuCeYUbpofYHZCPEVd17z8d2haji6G8rPvaK6GHJpjp8k-hJVtH5eiiI2KzEw77W6hjyVtKkqm9wI4WvMXmo_HJpgoRch0BWplyWpZfduYeFcWiCbgd1kuPVbgY0f6lyY_09v8hD-E6lkTWyycdMWoNoFVewF2xWHt-yeT3NbLelY9F1aNik2UIKFuyg; u=931702263491225; Hm_lvt_1db88642e346389874251b5a1eded6e3=1716359813; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1716359816; is_overseas=1',
        'DNT': '1',
        'Sec-Fetch-Dest': 'document',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'sec-ch-ua-platform': 'macOS',
    }
    now = datetime.now()

    # 格式化输出
    formatted_date = now.strftime("%Y_%m_%d")
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        json_string = json.dumps(response.json(), indent=4)
        with open('US_'+ formatted_date + '.json', 'w') as file:
            file.write(json_string)
    else:
        print("请求失败")

def get_stock_cn_list():
    url = 'https://stock.xueqiu.com/v5/stock/screener/quote/list.json?size=5000&order=desc&order_by=percent&market=US&type=sh_sz'
    headers = {
        'Cookie': 'device_id=e8a11bc724b7115f447ab4c08ef5beac; cookiesu=931702263491225; s=bg11zw7opj; __utma=1.1301827076.1710235788.1710235788.1710235788.1; __utmc=1; __utmz=1.1710235788.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); snbim_minify=true; smidV2=202403280849125de654b47643f6772e389fb8680f9a90000e32c4c27adc350; bid=aa3d1df992f90ca8cdd6773895b2f006_lus3wyz6; .thumbcache_f24b8bbe5a5934237bbc0eda20c1b6e7=p8It1Sxugp3IwvGKdOdGiotK7qnKgOccLoRu+Xp50bqyLzzUNN3C/MMnVnOYgTMcpdiRa5O/vqO9kIXKLJiK3g%3D%3D; acw_tc=276077a817163598111741134e6447a656c11bc217806699b22e5e4d5d4277; xq_a_token=c2aefa380b9072a563e961143570e259329d659f; xqat=c2aefa380b9072a563e961143570e259329d659f; xq_r_token=2823a23fcab28b5723fbd7c5220a4ba4cc755a52; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOi0xLCJpc3MiOiJ1YyIsImV4cCI6MTcxODg0NDcxMiwiY3RtIjoxNzE2MzU5NzU1MjU3LCJjaWQiOiJkOWQwbjRBWnVwIn0.VYFVyQFbyB3VYZqD2rSF5NHC8jErHCUTB5W59UetzZJ0128gPgfV7289QgUiOuoe5khAF5dOzY--abQFsEE-0IP_0yEfAI7G-qSUiWYBoyyy7yRgeh2x0imFsb8_lsBKItvQ1oFrar05CtlIOLUynWSaKoZuCeYUbpofYHZCPEVd17z8d2haji6G8rPvaK6GHJpjp8k-hJVtH5eiiI2KzEw77W6hjyVtKkqm9wI4WvMXmo_HJpgoRch0BWplyWpZfduYeFcWiCbgd1kuPVbgY0f6lyY_09v8hD-E6lkTWyycdMWoNoFVewF2xWHt-yeT3NbLelY9F1aNik2UIKFuyg; u=931702263491225; Hm_lvt_1db88642e346389874251b5a1eded6e3=1716359813; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1716359816; is_overseas=1',
        'DNT': '1',
        'Sec-Fetch-Dest': 'document',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'sec-ch-ua-platform': 'macOS',
    }
    now = datetime.now()

    # 格式化输出
    formatted_date = now.strftime("%Y_%m_%d")

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        json_string = json.dumps(response.json(), indent=4)
        with open('CN' + formatted_date + '.json', 'w') as file:
            file.write(json_string)
    else:
        print("请求失败")

def get_stock_cn_data():
    now = datetime.now()

    # 格式化输出
    formatted_date = now.strftime("%Y_%m_%d")

    with open('CN' + formatted_date + '.json', 'r') as file:
        data = json.load(file)
    symbols = [item['symbol'] for item in data['data']['list']]

    # 将数据转换为 DataFrame
    df = pd.DataFrame({'symbol': symbols})
    while True:
        try:
            for index, row in df.iterrows():
                print("Symbol:", row['symbol'], " Start")
                file_name = row['symbol'] + '.json'
                if os.path.exists(file_name):
                    print("JSON 文件已经存在，跳过...")
                else:
                    url = "https://stock.xueqiu.com/v5/stock/chart/kline.json?symbol="+ row['symbol'] + "&begin=1716449812285&period=day&type=before&count=-1000&indicator=kline,pe,pb,ps,pcf,market_capital,agt,ggt,balance"
                    print(url)
                    # 发送GET请求获取网页内容
                    headers = {
                        'Cookie': 'device_id=e8a11bc724b7115f447ab4c08ef5beac; cookiesu=931702263491225; s=bg11zw7opj; __utma=1.1301827076.1710235788.1710235788.1710235788.1; __utmc=1; __utmz=1.1710235788.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); snbim_minify=true; smidV2=202403280849125de654b47643f6772e389fb8680f9a90000e32c4c27adc350; bid=aa3d1df992f90ca8cdd6773895b2f006_lus3wyz6; .thumbcache_f24b8bbe5a5934237bbc0eda20c1b6e7=p8It1Sxugp3IwvGKdOdGiotK7qnKgOccLoRu+Xp50bqyLzzUNN3C/MMnVnOYgTMcpdiRa5O/vqO9kIXKLJiK3g%3D%3D; acw_tc=276077a817163598111741134e6447a656c11bc217806699b22e5e4d5d4277; xq_a_token=c2aefa380b9072a563e961143570e259329d659f; xqat=c2aefa380b9072a563e961143570e259329d659f; xq_r_token=2823a23fcab28b5723fbd7c5220a4ba4cc755a52; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOi0xLCJpc3MiOiJ1YyIsImV4cCI6MTcxODg0NDcxMiwiY3RtIjoxNzE2MzU5NzU1MjU3LCJjaWQiOiJkOWQwbjRBWnVwIn0.VYFVyQFbyB3VYZqD2rSF5NHC8jErHCUTB5W59UetzZJ0128gPgfV7289QgUiOuoe5khAF5dOzY--abQFsEE-0IP_0yEfAI7G-qSUiWYBoyyy7yRgeh2x0imFsb8_lsBKItvQ1oFrar05CtlIOLUynWSaKoZuCeYUbpofYHZCPEVd17z8d2haji6G8rPvaK6GHJpjp8k-hJVtH5eiiI2KzEw77W6hjyVtKkqm9wI4WvMXmo_HJpgoRch0BWplyWpZfduYeFcWiCbgd1kuPVbgY0f6lyY_09v8hD-E6lkTWyycdMWoNoFVewF2xWHt-yeT3NbLelY9F1aNik2UIKFuyg; u=931702263491225; Hm_lvt_1db88642e346389874251b5a1eded6e3=1716359813; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1716359816; is_overseas=1',
                        'DNT': '1',
                        'Sec-Fetch-Dest': 'document',
                        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                        'sec-ch-ua-platform': 'macOS',
                    }
                    response = requests.get(url, headers=headers)

                    # 检查请求是否成功
                    if response.status_code == 200:
                        json_string = json.dumps(response.json(), indent=4)
                        with open(file_name, 'w') as file:
                            file.write(json_string)
                        print("Symbol:", row['symbol'], " Done")
                    else:
                        print("请求失败")
                    time.sleep(0.1)
        except ConnectionError as e:
            # 捕获连接错误并打印错误信息
            print("连接错误:", e)
            # 等待一段时间后重试
            time.sleep(5)  # 可以根据需要调整等待时间
# 调用函数并传入URL
get_stock_cn_list()
get_stock_cn_data()

    # 从 JSON 文件加载数据到 DataFrame
