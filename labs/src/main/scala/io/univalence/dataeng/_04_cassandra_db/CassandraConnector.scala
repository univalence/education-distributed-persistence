package io.univalence.dataeng._04_cassandra_db

import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.{Config, ConfigFactory}
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName

import scala.jdk.CollectionConverters.SeqHasAsJava

import java.net.InetSocketAddress

class CassandraContainer(dockerImageName: DockerImageName)
    extends GenericContainer[CassandraContainer](dockerImageName) {}

object CassandraConnector {
  val cassandraConf: Config = ConfigFactory.load("cassandra.conf")

  // cassandra config
  lazy val dataCenter: String        = cassandraConf.getString("cassandra.data-center")
  lazy val cassandraHost: String     = cassandraConf.getString("cassandra.hosts")
  lazy val cassandraPorts: Seq[Int]  = cassandraConf.getString("cassandra.ports").split(",").toSeq.map(_.trim.toInt)
  lazy val cassandraKeyspace: String = cassandraConf.getString("cassandra.keyspace")

  def usingCassandra(f: CqlSession => Unit): Unit = {
    case class CassandraConfiguration(seed: String, port: Int)

    val dockerImage          = DockerImageName.parse("cassandra").withTag("4.0")
    val cassandraClusterName = "cassandra-cluster"

    val cassandraSeeds = cassandraPorts.indices.map(i => s"cassandra-node${i + 1}")

    val containers: Seq[CassandraContainer] =
      cassandraSeeds.zip(cassandraPorts).map { case (seed, port) =>
        new CassandraContainer(dockerImage)
          .withCreateContainerCmdModifier(cmd => cmd.withName(seed))
          .withEnv("CASSANDRA_CLUSTER_NAME", cassandraClusterName)
          .withExposedPorts(port)
      }

    containers.foreach(_.start())

    val builder = CqlSession.builder()
    builder.addContactPoints(
      containers
        .zip(cassandraPorts)
        .map { case (container, port) => new InetSocketAddress(cassandraHost, container.getMappedPort(port)) }
        .asJava
    )
    builder.withLocalDatacenter(dataCenter)
    val session = builder.build()

    try f(session)
    finally {
      session.close()
      containers.foreach(_.close())
    }
  }
}
