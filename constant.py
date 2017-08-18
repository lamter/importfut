import pytz
import datetime

# 日盘开始和结束时间
DAY_TRADING_START_TIME = datetime.time(9)
DAY_TRADING_END_TIME = datetime.time(15, 15)

LOCAL_TIMEZONE = pytz.timezone('Asia/Shanghai')
