import logging
import os


class OriginBar(object):
    """
    原始数据实例
    """

    def __init__(self, year, path):
        self.log = logging.getLogger('root')
        self.year = year
        self.path = path
        self.symbol = os.path.split(path)[-1].strip('.csv')

    def __str__(self):
        s = object.__str__(self)
        s = s.strip('>')
        s += ' year: {} symbol: {}>'.format(self.year, self.symbol)
        return s

