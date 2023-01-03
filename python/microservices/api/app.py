import json
from http import HTTPStatus

from cassandra.cluster import Cluster
from flask import Flask, Response
from flask_classful import FlaskView, route

from microservices.common import config
from microservices.common.repository import StockRepository


class ApiView(FlaskView):
    repository: StockRepository = None

    @route("/stocks", methods=['GET'])
    def all_stocks(self):
        results = self.__class__.repository.find_all()

        return Response(json.dumps([s.toDict() for s in results]), mimetype="application/json")

    @route("/stocks/_count", methods=['GET'])
    def count_stocks(self):
        result = len(self.__class__.repository.find_all())

        return Response(json.dumps({'count': result}), mimetype="application/json")

    @route("/stocks/<id>", methods=['GET'])
    def stock(self, id: str):
        result = self.__class__.repository.find_by_id(id)

        if result is None:
            return Response(json.dumps({'id': id, 'error': "not found"}), mimetype="application/json",
                            status=HTTPStatus.NOT_FOUND)
        else:
            return Response(json.dumps(result.toDict()), mimetype="application/json")


if __name__ == '__main__':
    port = config.apiPort

    cluster = Cluster()
    connection = cluster.connect()
    try:
        app = Flask(__name__)
        ApiView.repository = StockRepository(connection)
        ApiView.register(app, route_base="/api")
        app.run("0.0.0.0", port)
    finally:
        cluster.shutdown()
