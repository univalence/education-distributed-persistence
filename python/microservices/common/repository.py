from typing import *

from cassandra.cluster import Session

from microservices.common import config
from microservices.common.stock import StockInfo


class StockRepository(object):
    def __init__(self, connection: Session):
        self.__connection = connection

    def find_by_id(self, id: str) -> Optional[StockInfo]:
        row = self.__connection.execute(
            "SELECT id, ts, qtt FROM " + config.cassandra_table + " WHERE id = %s",
            (id,)
        ).one()

        if row is None:
            return None
        else:
            (id, ts, qtt) = row
            return StockInfo(id=id, ts=ts, quantity=qtt)

    def find_all(self) -> List[StockInfo]:
        rows = self.__connection.execute(
            "SELECT id, ts, qtt FROM " + config.cassandra_table
        ).all()

        result = []
        for (id, ts, qtt) in rows:
            result.append(StockInfo(id=id, ts=ts, quantity=qtt))

        return result

    def save(self, stock: StockInfo):
        self.__connection.execute(
            "INSERT INTO " + config.cassandra_table + " (id, ts, qtt) VALUES (%s, %s, %s)",
            (stock.id, stock.ts, stock.quantity)
        )
