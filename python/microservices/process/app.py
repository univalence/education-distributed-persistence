import json
import time
from typing import *

from cassandra.cluster import Cluster, Session
from confluent_kafka import Consumer

from microservices.common import config
from microservices.common.stock import StockInfo


class StockConsumer(object):
    def __init__(self, consumer: Consumer, topic: str):
        self.__consumer = consumer
        self.__consumer.subscribe([topic])

    def next_stock(self) -> Optional[StockInfo]:
        def now() -> int:
            return int(time.time_ns() / 1000000)

        message = consumer.poll(0.5)
        if message is None:
            return None
        elif message.error():
            print("Error: %s" % message.error().str())
            return None
        else:
            key = message.key().decode("utf-8")
            value = json.loads(message.value().decode("utf-8"))
            ts = now()

            return StockInfo(id=key, ts=ts, quantity=int(value['quantity']))


class StockRepository(object):
    def __init__(self, connection: Session):
        self.__connection = connection

    def save(self, stock: StockInfo):
        connection.execute(
            "INSERT INTO " + config.cassandra_table + " (id, ts, qtt) VALUES (%s, %s, %s)",
            (stock.id, stock.ts, stock.quantity)
        )


class ProcessService(object):
    def __init__(self, consumer: StockConsumer, connection: StockRepository):
        self.__consumer = consumer
        self.__connection = connection

    def get_and_save_stock(self):
        stock = self.__consumer.next_stock()
        if stock is not None:
            ts = time.asctime(time.localtime())
            print("got %s --> save @ %s" % (stock, ts))
            self.__connection.save(stock)


if __name__ == '__main__':
    stock_topic = config.ingestTopic
    stock_keyspace = "store"
    group_id = "process-group-0"

    consumer = Consumer({
        'bootstrap.servers': config.kafkaHost,
        'group.id': group_id,
        'default.topic.config': {'auto.offset.reset': 'earliest'}
    })

    cluster = Cluster()

    try:
        connection = cluster.connect()

        stock_repository = StockRepository(connection)
        stock_consumer = StockConsumer(consumer, stock_topic)
        service = ProcessService(stock_consumer, stock_repository)

        while True:
            service.get_and_save_stock()

    finally:
        cluster.shutdown()
        consumer.close()
