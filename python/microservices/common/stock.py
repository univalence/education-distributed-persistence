import json


class Stock(object):
    id: str
    quantity: float

    def __init__(self, id: str, quantity: float):
        self.id = id
        self.quantity = quantity

    def __str__(self):
        return "%s(%s,%s)" % (self.__class__.__name__, self.id, self.quantity)

    def toDict(self):
        return {'id': self.id, 'quantity': self.quantity}

    def toJson(self):
        return json.dumps(self.toDict())


class StockInfo(object):
    id: str
    ts: int
    quantity: int

    def __init__(self, id: str, ts: int, quantity: int):
        self.id = id
        self.ts = ts
        self.quantity = quantity

    def __str__(self):
        return "%s(%s,%s,%s)" % (self.__class__.__name__, self.id, self.ts, self.quantity)

    def toDict(self):
        return {'id': self.id, 'ts': self.ts, 'quantity': self.quantity}

    def toJson(self):
        return json.dumps(self.toDict())
