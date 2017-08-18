import os
import logging.config
import logging
from bson.codec_options import CodecOptions
import time

import pymongo
import pandas as pd
import numpy as np

from originbar import OriginBar
from constant import *


class MainEngine(object):
    """
    操作实例
    """

    def __init__(self, futpath, mongoConf, logconfig):
        logging.config.dictConfig(logconfig)
        self.log = logging.getLogger('root')

        self.mongoConf = mongoConf
        self.futpath = futpath
        self.path = {}  # 原始 CSV 数据的路径

        self.collection = None

    def init(self):
        """

        :return:
        """
        # 链接数据库
        self.dbConnect()

        # 加载文件路径
        self.loaddir()

    def start(self):
        """

        :return:
        """
        count = 0
        for year, originBars in self.path.items():
            for ob in originBars:
                # 清洗数据
                if '主力' in ob.symbol:
                    # 暂时不处理 主力连续 和 次主力连续合约
                    continue

                # 生成 DataFrame 数据
                df = self.getBarDf(ob)

                # # # 添加结算日
                df = self.setTradingDay(df)

                # 清洗没有成交的数据
                df = self.clearNoTrader(df)

                # 保存数据
                documents = df.to_dict('records')
                self.collection.insert_many(documents)
                self.log.info("{} 保存了 {} 条数据到mongodb".format(ob.symbol, df.shape[0]))
                count += df.shape[0]
                time.sleep(1)

            self.log.info('year {} 累积保存了 {} 条数据'.format(year, count))

    def dbConnect(self):
        mongoConf = self.mongoConf

        db = pymongo.MongoClient(
            mongoConf['host'],
            mongoConf['port']
        )[mongoConf['dbn']]

        if mongoConf.get('username') is not None:
            # 登陆授权
            db.authenticate(mongoConf['username'], mongoConf['password'])

        # 确认登陆
        db.client.server_info()

        # 设置 collection 的时区生效
        self.collection = db[mongoConf['collection']].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=LOCAL_TIMEZONE))

    def loaddir(self):
        """

        :return:
        """
        root, dirs, files = next(os.walk(self.futpath))

        dirs.sort(reverse=True)
        for yearDir in dirs:

            year = yearDir[-4:]  # 年份
            yearDirPath = os.path.join(root, yearDir)  # 文件按年打包
            yearDirPath, dirs, files = next(os.walk(yearDirPath))
            originBars = []
            self.path[year] = originBars
            for f in files:
                if not f.endswith('.csv'):
                    # 忽略非 csv 文件
                    continue
                # if __debug__ and 'rb1710' not in f:
                #     # 调试指定合约
                #     continue

                path = os.path.join(yearDirPath, f)
                ob = OriginBar(year, path)
                originBars.append(ob)

                # if __debug__:
                #     self.log.warning('只取一个文件 {}'.format(ob.symbol))
                #     return

    def getBarDf(self, ob):
        df = pd.read_csv(ob.path, encoding='GB18030')

        # 去掉多余的字段
        del df['市场代码']
        del df['成交额']
        df.columns = ['symbol', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'openInterest']

        df.datetime = pd.to_datetime(df.datetime)

        df['date'] = df.datetime.apply(lambda dt: dt.strftime('%Y%m%d'))
        df['time'] = df.datetime.apply(lambda dt: dt.time())

        return df

    def clearNoTrader(self, df):
        """
        清洗没有成交的 bar
        :param df:
        :return:
        """

        return df[df.volume != 0]

    def setTradingDay(self, df):
        # 白天的数据，就是交易日

        td = df.time.apply(lambda t: DAY_TRADING_START_TIME <= t <= DAY_TRADING_END_TIME)
        tds = []
        for i, isTading in enumerate(td):
            if isTading:
                tds.append(df.date[i])
            else:
                tds.append(np.nan)

        # 空白的bar，使用最近一个有效数据来作为交易日
        tradingDay = pd.Series(tds).fillna(method='bfill')
        # 时区
        tradingDay = pd.Index(pd.to_datetime(tradingDay)).tz_localize('Asia/Shanghai')
        df['tradingDay'] = tradingDay
        df.datetime = pd.Index(df.datetime).tz_localize('Asia/Shanghai')

        df['time'] = df.datetime.apply(lambda dt: dt.strftime('%H:%M:%S'))
        return df
