from cassandra.cluster import Cluster

if __name__ == '__main__':
    cassandra = Cluster()
    connection = cassandra.connect()
    try:
        connection.execute("""CREATE KEYSPACE IF NOT EXISTS store
            WITH REPLICATION = {
              'class': 'SimpleStrategy',
              'replication_factor': 1
            }""")

        connection.execute("""CREATE TABLE IF NOT EXISTS store.stock (
              id TEXT,
              ts BIGINT,
              qtt INT,
            
              PRIMARY KEY (id)
            )""")
    finally:
        connection.shutdown()
