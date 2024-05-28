from clickhouse_driver import Client
from datetime import datetime,timedelta
import backtrader as bt
import pandas as pd
import talib as ta
import numpy as np
import matplotlib.pyplot as plt
import pyfolio as pf

plt.rcParams['axes.unicode_minus']=False
plt.rcParams['figure.figsize']=[10, 8]
plt.rcParams['figure.dpi']=200
plt.rcParams['figure.facecolor']='w'
plt.rcParams['figure.edgecolor']='k'

def convert_timestamp_to_date(timestamp_ms):
    date = datetime.utcfromtimestamp(timestamp_ms / 1000)  # 将毫秒转换为秒并使用utcfromtimestamp函数转换为日期
    return date

def format_data(result):
        # 将ClickHouse查询结果转换为Pandas DataFrame
        # 假设数据包含'timestamp', 'open', 'high', 'low', 'close', 'volume'
        df = pd.DataFrame(result, columns=['datetime', 'open', 'high', 'low', 'close', 'volume'])
        #df['datetime'] = df['datetime'].apply(convert_timestamp_to_date)
        return df

class ClickHouseData(bt.feeds.DataBase):
    params = (
        ('host', '192.168.3.10'),
        ('database', 'default'),
        ('table', 'shsz_stock_daily'),
        ('port', 9000),
    )

    def start(self):
        self.client = Client(host=self.p.host, database=self.p.database, port=self.p.port)

    def stop(self):
       pass  # 如果需要在停止时执行清理操作，可以在这里添加代码

    def _load(self):
        query = f"SELECT timestamp, open, high, low, close, volume, 0 FROM {self.p.table} where symbol = 'SZ300706' order by timestamp asc"
        result = self.client.execute(query)
        df = self._format_data(result)
        print(df)
        start_time = datetime(2020, 4, 6)
        end_time = datetime(2024, 5, 21)

        bt.feeds.PandasData(dataname=df, fromdate=start_time, todate=end_time)
    
        #self._load_from_dataframe(df)

    def _format_data(self, result):
        # 将ClickHouse查询结果转换为Pandas DataFrame
        # 假设数据包含'timestamp', 'open', 'high', 'low', 'close', 'volume'
        df = pd.DataFrame(result, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'openinterest'])
        df['timestamp'] = df['timestamp'].apply(convert_timestamp_to_date)
        return df

class MyStrategy(bt.Strategy):
    params = dict(
        N1= 40, # 唐奇安通道上轨的t
        N2=30, # 唐奇安通道下轨的t
        N3=20,# ADX的强度值
        N4=20, # ADX的时间周期参数
        printlog=False, 
        )
    
    def log(self, txt, dt=None,doprint=False):
        dt = dt or self.datas[0].datetime.date(0)
        print(f'{dt.isoformat()},{txt}')

    def __init__(self): 
        self.order = None                   
        self.buy_count = 0 # 记录买入次数
        self.last_price = 0 # 记录买入价格
        self.close = self.datas[0].close
        self.high = self.datas[0].high
        self.low = self.datas[0].low
        # 计算唐奇安通道上轨：过去20日的最高价
        self.DonchianH = bt.ind.Highest(self.high(-1), period=int(self.p.N1), subplot=True)
        # 计算唐奇安通道下轨：过去10日的最低价
        self.DonchianL = bt.ind.Lowest(self.low(-1), period=int(self.p.N2), subplot=True)
        # 计算唐奇安通道中轨
        self.DonchianM= (self.DonchianH+self.DonchianL)/2
        
        # 生成唐奇安通道上轨突破：close>DonchianH，取值为1.0；反之为 -1.0
        self.CrossoverH = bt.ind.CrossOver(self.close(0), self.DonchianH, subplot=False)
        # 生成唐奇安通道下轨突破:
        self.CrossoverL = bt.ind.CrossOver(self.close(0), self.DonchianL, subplot=False)
        # 生成唐奇安通道中轨突破:
        self.CrossoverM = bt.ind.CrossOver(self.close(0), self.DonchianM, subplot=False)

        # 计算 ADX，直接调用 talib 
        self.ADX = bt.talib.ADX(self.high, self.low, self.close,timeperiod=int(self.p.N4),subplot=True)
#         self.log(self.ADX[-1],doprint=True)
        print(self.ADX)
    def next(self): 
#         self.log(self.ADX[0],doprint=True)
        # 如果还有订单在执行中，就不做新的仓位调整
        if self.order:
            return  
                
        # 如果当前持有多单
        if self.position.size > 0 :
            # 平仓设置
            if self.CrossoverM<0 or self.ADX[0]<self.ADX[-1]:
                self.order = self.sell(size=abs(self.position.size))
                self.buy_count = 0 
                
        # 如果当前持有空单
        # elif self.position.size < 0 :
            # 平仓设置
        #    if self.CrossoverM>0 or self.ADX[0]<self.ADX[-1]:
        #        self.order = self.buy(size=abs(self.position.size))
        #        self.buy_count = 0 
                
        else: # 如果没有持仓，等待入场时机
            #入场: 价格突破上轨线且空仓时，做多
            if self.CrossoverH > 0 and self.buy_count == 0 and self.ADX[0]>self.ADX[-1] and self.ADX[-1]>int(self.p.N3):
                self.buy_unit = int(self.broker.getvalue()/self.close[0]/4)
#                 self.buy_unit=500
                self.order = self.buy(size=self.buy_unit)
                self.last_price = self.position.price # 记录买入价格
                self.buy_count = 1  # 记录本次交易价格
            #入场: 价格跌破下轨线且空仓时，做空
            #elif self.CrossoverL < 0 and self.buy_count == 0 and self.ADX[0]>self.ADX[-1] and self.ADX[-1]>int(self.p.N3):
            #    self.buy_unit = int(self.broker.getvalue()/self.close[0]/4)
            #    self.buy_unit=500
            #    self.order = self.sell(size=self.buy_unit)
            #    self.last_price = self.position.price # 记录买入价格
            #    self.buy_count = 1  # 记录本次交易价格
                
    def notify_order(self, order):
        order_status = ['Created','Submitted','Accepted','Partial',
                        'Completed','Canceled','Expired','Margin','Rejected']
        # 未被处理的订单
        if order.status in [order.Submitted, order.Accepted]:
            self.log('ref:%.0f, name: %s, Order: %s'% (order.ref,
                                                   order.data._name,
                                                   order_status[order.status]))
            return
        # 已经处理的订单
        if order.status in [order.Partial, order.Completed]:
            if order.isbuy():
                self.log(
                        'BUY EXECUTED, status: %s, ref:%.0f, name: %s, Size: %.2f, Price: %.2f, Cost: %.2f, Comm %.2f' %
                        (order_status[order.status], # 订单状态
                         order.ref, # 订单编号
                         order.data._name, # 股票名称
                         order.executed.size, # 成交量
                         order.executed.price, # 成交价
                         order.executed.value, # 成交额
                         order.executed.comm)) # 佣金
            else: # Sell
                self.log('SELL EXECUTED, status: %s, ref:%.0f, name: %s, Size: %.2f, Price: %.2f, Cost: %.2f, Comm %.2f' %
                            (order_status[order.status],
                             order.ref,
                             order.data._name,
                             order.executed.size,
                             order.executed.price,
                             order.executed.value,
                             order.executed.comm))
                    
        elif order.status in [order.Canceled, order.Margin, order.Rejected, order.Expired]:
            # 订单未完成
            self.log('ref:%.0f, name: %s, status: %s'% (
                order.ref, order.data._name, order_status[order.status]))
            
        self.order = None
        
    def notify_trade(self, trade):
        # 交易刚打开时
        if trade.justopened:
            self.log('Trade Opened, name: %s, Size: %.2f,Price: %.2f' % (
                    trade.getdataname(), trade.size, trade.price))
        # 交易结束
        elif trade.isclosed:
            self.log('Trade Closed, name: %s, GROSS %.2f, NET %.2f, Comm %.2f' %(
            trade.getdataname(), trade.pnl, trade.pnlcomm, trade.commission))
        # 更新交易状态
        else:
            self.log('Trade Updated, name: %s, Size: %.2f,Price: %.2f' % (
                    trade.getdataname(), trade.size, trade.price))
    def stop(self):
        self.log(f'(组合线：{self.p.N1},{self.p.N2},{self.p.N3},{self.p.N4}); 期末总资金: {self.broker.getvalue():.2f}', doprint=False)

if __name__ == '__main__':
    params = (
        ('host', '192.168.3.10'),
        ('database', 'default'),
        ('table', 'shsz_stock_daily'),
        ('port', 9000),
    )
    cerebro = bt.Cerebro()
    client = Client(host='192.168.3.10', database='default', port=9000)
    query = f"SELECT timestamp, open, high, low, close, volume FROM cn_stock_daily where symbol = 'SH600938' order by timestamp asc"
    result = client.execute(query)
    df = format_data(result)
    df.index = [datetime.fromtimestamp(x / 1000.0) for x in df.datetime]
    print(df)
    start_time = datetime(2020, 4, 6)
    end_time = datetime(2024, 5, 21)

    data = bt.feeds.PandasData(dataname=df, fromdate=start_time, todate=end_time)

    cerebro = bt.Cerebro()
    cerebro.addstrategy(MyStrategy)   
    cerebro.adddata(data)
    #broker设置资金、手续费
    cerebro.broker.setcash(10000)
    cerebro.broker.setcommission(commission=0.02)
    #设置指标观察
    cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')
    print('期初总资金: %.2f' % cerebro.broker.getvalue())
    results=cerebro.run(maxcpus=2)
    cerebro.plot(iplot=False)
    result = results[0]
    pyfolio = result.analyzers.pyfolio # 注意：后面不要调用 .get_analysis() 方法
    # 或者是 result[0].analyzers.getbyname('pyfolio')
    returns, positions, transactions, gross_lev = pyfolio.get_pf_items()
    pf.create_full_tear_sheet(returns)

    #cerebro.adddata(data)
    #cerebro.addstrategy(MyStrategy)
    #cerebro.run()