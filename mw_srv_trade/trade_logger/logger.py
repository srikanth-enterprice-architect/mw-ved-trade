import logging

cus_logger = logging.getLogger(__name__)
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler('mw-ved-trade.log')

c_format = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s - %(funcName)10s:%(lineno)s - %(message)s')
f_format = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s - %(funcName)10s:%(lineno)s - %(message)s')

c_handler.setFormatter(c_format)
f_handler.setFormatter(f_format)

cus_logger.addHandler(c_handler)
cus_logger.addHandler(f_handler)
