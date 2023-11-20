from typing import NamedTuple, Any, Iterator
from datetime import datetime


class Record(NamedTuple):
    key: str
    value: Any
    timestamp: datetime
    deleted: bool

    def __repr__(self) -> str:
        return """Record(key="%s",value=%s,timestamp=%s,deleted=%s)""" % (
            self.key,
            self.value,
            self.timestamp.isoformat(),
            self.deleted
        )


class Store(object):
    def put(self, key: str, value: Any) -> None:
        """Add or update a record."""
        pass

    def get(self, key: str) -> Record:
        """Try to get a record."""
        pass

    def delete(self, key: str) -> None:
        """Delete a record. The delete operation will succeed even if
        the key is not present."""
        pass

    def scan(self) -> Iterator[Record]:
        """Get all records."""
        pass

    def scan_from(self, key: str) -> Iterator[Record]:
        """Get all records starting from a key."""
        pass

    def get_prefix(self, prefix: str) -> Iterator[Record]:
        """Get all records which keys have the same prefix."""
        pass


class InMemoryStore(Store):
    def __init__(self, data: dict[str, Record] = {}) -> None:
        # Python does not have a sorted dict, so we have to cheat a
        # little bit by using a dict with a list containing the key and
        # the we sort on every PUT
        self.__data: dict[str, Record] = data.copy()
        self.__keys: list[str] = sorted(self.__data.keys())

    def put(self, key: str, value: Any) -> None:
        record = Record(
            key=key,
            value=value,
            timestamp=datetime.now(),
            deleted=False
        )
        self.__data[key] = record
        self.__keys.append(key)
        self.__keys.sort()

    def get(self, key: str) -> Record:
        if (key not in self.__data or self.__data[key].deleted):
            raise KeyError(key)
        return self.__data[key]

    def delete(self, key: str) -> None:
        # ensure delete is reentrant
        if (key not in self.__data or self.__data[key].deleted):
            return
        self.__data[key] = self.__data[key]._replace(deleted=True)

    def scan(self) -> Iterator[Record]:
        for k in self.__keys:
            yield self.__data[k]

    def scan_from(self, key: str) -> Iterator[Record]:
        for k in self.__keys:
            if k > key:
                yield self.__data[k]

    def get_prefix(self, prefix: str) -> Iterator[Record]:
        for k in self.__keys:
            if k.startswith(prefix):
                yield self.__data[k]
