package io.univalence.dataeng._05_elasticsearch

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.ElasticsearchTransport
import co.elastic.clients.transport.rest_client.RestClientTransport
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.utility.DockerImageName

object ElasticSearchConnector {

  /**
   * A wrapper to run code using an ElasticSearch container and an
   * ElasticSearch connection.
   */
  def usingElasticSearch(f: (ElasticsearchClient, JsonMapper) => Unit): Unit = {
    val serverPort = 9200

    val dockerImage            = DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:7.17.0")
    val currentDirectory       = new java.io.File(".").getCanonicalPath
    val elasticsearchDirectory = "/usr/share/elasticsearch/data/nodes/0"
    val databaseDirectory      = s"$currentDirectory/data/target/_05_elasticsearch"

    val elasticsearch: ElasticsearchContainer =
      new ElasticsearchContainer(dockerImage)
        .withFileSystemBind(databaseDirectory, elasticsearchDirectory) // Link local file to the docker file
        .withEnv("xpack.security.enabled", "false")                    // Disable security log

    elasticsearch.start()

    /**
     * We create a mapper object, which tell to Jackson how to
     * serialize/deserialize JSON.
     */
    val mapper: JsonMapper =
      JsonMapper
        .builder()
        .addModule(DefaultScalaModule)
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .build()

    /**
     * ==Initialization==
     * We first initialize the Api client to interact with
     * Elasticsearch.
     */
    val restClient: RestClient =
      RestClient.builder(new HttpHost("localhost", elasticsearch.getMappedPort(serverPort))).build()
    val transport: ElasticsearchTransport = new RestClientTransport(restClient, new JacksonJsonpMapper(mapper))
    val client: ElasticsearchClient       = new ElasticsearchClient(transport)

    try f(client, mapper)
    finally {
      transport.close()
      restClient.close()
      elasticsearch.stop()
    }
  }
}
