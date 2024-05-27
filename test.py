from clickhouse_driver import Client
import backtrader as bt

# 连接到 ClickHouse
client = Client('192.168.3.10', port=9000, database='default')

# 查询数据
query = 'SELECT count(*) FROM shsz_stock_daily'
result = client.execute(query)
print(result)