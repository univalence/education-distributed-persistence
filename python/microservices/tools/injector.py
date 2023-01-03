import requests

from microservices.common.stock import Stock
from microservices.common import config

import random
import time


class StockSender(object):
    def __init__(self, host: str, port: int):
        self.__host = host
        self.__port = port

    def send(self, stock: Stock):
        data = stock.toJson()
        response = requests.post(
            "http://%s:%s/api/stocks/%s" % (self.__host, self.__port, stock.id),
            json=data
        )
        try:
            print("Sent with %s %s: %s" % (response.status_code, response.reason, data))
        finally:
            response.close()


if __name__ == '__main__':
    stock_ids = range(0, 20)
    host = "localhost"
    port = config.ingestPort

    sender = StockSender(host, port)

    while True:
        id = str(random.choice(stock_ids))
        quantity = random.randint(1, 100)
        stock = Stock(id=id, quantity=quantity)
        sender.send(stock)
        time.sleep(1)
