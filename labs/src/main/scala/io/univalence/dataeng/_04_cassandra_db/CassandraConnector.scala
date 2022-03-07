package io.univalence.dataeng._04_cassandra_db

import com.datastax.oss.driver.api.core.CqlSession

import java.net.InetSocketAddress
import scala.jdk.javaapi.CollectionConverters

object CassandraConnector extends CassandraConf {

  def usingCassandra(f: CqlSession => Unit): Unit = {
    val builder = CqlSession.builder()
    builder.addContactPoints(
      CollectionConverters.asJavaCollection(Seq(
        new InetSocketAddress(cassandraHosts(0), cassandraPorts(0)),
        new InetSocketAddress(cassandraHosts(0), cassandraPorts(1))
      ))
    )
    builder.withLocalDatacenter(dataCenter)
    val session = builder.build()

    try f(session)
    finally {
      session.close()
    }
  }
}
