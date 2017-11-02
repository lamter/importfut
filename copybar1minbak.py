"""
从 ctp.bar1min_bak 复制数据到本地
"""
import datetime
import pymongo
import json
import arrow
import pytz
from bson.codec_options import CodecOptions
import time

LOCAL_TIMEZONE = pytz.timezone('Asia/Shanghai')


class Mover(object):
    """

    """

    def __init__(self, mongoConf, startDate, endDate):
        self.mongoConf = mongoConf
        self.startDate = startDate
        self.endDate = endDate
        if self.startDate < self.endDate:
            raise ValueError(u'startDate 应该当大于 endDate')

        # 本地collection
        self.targetCol = None
        # 远程 collection
        self.sourceCol = None

    def dbConnect(self):
        self.targetCol = self._initMongo(self.mongoConf['targetMongo'])
        self.sourceCol = self._initMongo(self.mongoConf['sourceMongo'])

    def _initMongo(self, conf):
        client = pymongo.MongoClient(
            host=conf['host'],
            port=conf['port']
        )
        db = client[conf['dbn']]

        # 登陆
        if conf.get('username'):
            db.authenticate(conf['username'], conf['password'])

        return db[conf['collection']].with_options(
            codec_options=CodecOptions(tz_aware=True, tzinfo=LOCAL_TIMEZONE))

    def init(self):
        self.dbConnect()

    def start(self):
        dt = self.startDate
        while dt >= self.endDate:
            print(dt)
            sql = {
                'tradingDay': dt,
            }
            cursor = self.sourceCol.find(sql, {'_id': 0})
            count = 0
            documents = []
            amount = 0


            for d in cursor:
                count += 1
                amount += 1
                documents.append(d)
                if count > 3000:
                    print(amount)
                    self.targetCol.insert_many(documents)
                    count = 0
                    documents = []
                    time.sleep(0.1)
            if documents:
                print(amount)
                self.targetCol.insert_many(documents)
                time.sleep(0.1)


            dt -= datetime.timedelta(days=1)

if __name__ == '__main__':
    settingFile = 'conf/copy.json'
    loggingConfigFile = 'conf/logconfig.json'
    # serverChanFile = 'conf/serverChan.json'

    if __debug__:
        settingFile = 'tmp/copy.json'
        loggingConfigFile = 'tmp/logconfig.json'

    # with open(serverChanFile, 'r') as f:
    #     serverChanUrls = json.load(f)['serverChanSlaveUrls']

    with open(settingFile, 'r') as f:
        kwargs = json.load(f)

    with open(loggingConfigFile, 'r') as f:
        logConfig = json.load(f)

    startDate = arrow.get('2017-11-01 00:00:00+08:00').datetime
    endDate = arrow.get('2011-01-01 00:00:00+08:00').datetime
    endDate = None
    mainEngine = Mover(startDate=startDate, endDate=endDate,**kwargs)
    mainEngine.init()
    mainEngine.start()
