package io.univalence.dataeng._04_cassandra_db

import com.typesafe.config.ConfigFactory

trait CassandraConf {
  val cassandraConf = ConfigFactory.load("cassandra.conf")

  // cassandra config
  lazy val dataCenter = cassandraConf.getString("cassandra.data-center")
  lazy val cassandraHosts = cassandraConf.getString("cassandra.hosts").split(",").toSeq.map(_.trim)
  lazy val cassandraPorts = cassandraConf.getString("cassandra.ports").split(",").toSeq.map(_.trim.toInt)
  lazy val cassandraKeyspace = cassandraConf.getString("cassandra.keyspace")

}
