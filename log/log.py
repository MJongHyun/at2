### 로그 추출 및 저장 ###

import logging
import io
from datetime import datetime

class log:

    def __init__(self):
        self.today_date = datetime.today().strftime('%Y-%m-%d')
        self.logger = logging.getLogger('AT2.log')
        self.logger.setLevel(logging.INFO)
        self.form = '[%(asctime)s] [%(levelname)s] [%(filename)s] [%(funcName)s] [%(lineno)s] [%(message)s]'
        self.format = logging.Formatter(self.form, datefmt='%Y-%m-%d %H:%M:%S')

        self.errors = self.errors = io.StringIO()
        self.consoleHandler = logging.StreamHandler(self.errors)
        self.consoleHandler.setFormatter(self.format)
        self.logger.addHandler(self.consoleHandler)

        self.filePath = 'log/AT2_{}.log'.format(self.today_date)
        self.fileHander = logging.FileHandler(self.filePath)
        self.fileHander.setFormatter(self.format)
        self.logger.addHandler(self.fileHander)
