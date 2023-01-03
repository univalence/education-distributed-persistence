from http import HTTPStatus

from confluent_kafka import Producer
from flask import Flask, request, Response
from flask_classful import FlaskView, route

from microservices.common import config
from microservices.common.stock import Stock

import json


class StockProducer(object):
    def __init__(self, producer: Producer, topic: str):
        self.__producer = producer
        self.__topic = topic

    def send(self, stock: Stock):
        self.__producer.produce(topic=self.__topic, key=stock.id, value=stock.toJson())
        self.__producer.flush(0.5)


class IngestView(FlaskView):
    producer: StockProducer = None

    @route("/stocks/<id>", methods=['POST'])
    def stock(self, id):
        quantity = request.json['quantity']
        stock = Stock(id=id, quantity=quantity)
        print("ingesting stock: %s" % stock)
        self.__class__.producer.send(stock)

        return Response(status=HTTPStatus.OK)


def main():
    topic = config.ingestTopic
    port = config.ingestPort
    route_base = "/api"

    producer = Producer({'bootstrap.servers': config.kafkaHost})
    IngestView.producer = StockProducer(producer, topic)

    app = Flask(__name__)
    IngestView.register(app, route_base=route_base)
    app.run("0.0.0.0", port, debug=True)


if __name__ == '__main__':
    main()
