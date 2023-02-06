package io.univalence.dataeng.internal

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, ListTopicsOptions, NewTopic, OffsetSpec}
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TopicExistsException

import io.univalence.dataeng.internal.utils.{isPrintable, retry, using}

import scala.util.Try

import java.time.{Instant, LocalDateTime, ZoneId}
import java.util
import java.util.concurrent.ExecutionException

object kafka_utils {

  import scala.jdk.CollectionConverters._

  def render(metadata: RecordMetadata): String =
    s"topic=${metadata.topic()} - partition=${metadata.partition()} - offset=${metadata.offset()}"

  def displayRecord[K, V](record: ConsumerRecord[K, V]): String = {
    val headers =
      if (record.headers().asScala.isEmpty) ""
      else
        record
          .headers()
          .asScala
          .map(header =>
            s"    ${header.key()} = ${header.value().map(b => if (isPrintable(b.toChar)) b.toChar else ".").mkString}"
          )
          .mkString("\n")

    val timestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()), ZoneId.systemDefault())

    s"""topic=${record.topic()} - partition=${record.partition()} - offset=${record.offset()} - ${record
        .timestampType()}=$timestamp$headers
       |  key: ${record.key()}
       |  value: ${record.value()}""".stripMargin
  }

  def withAdminClientOn[A](bootstrap: String)(f: AdminClient => A): A = {
    val properties =
      Map[String, AnyRef](
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap
      ).asJava

    using(AdminClient.create(properties))(f)
  }

  def createTopics(topics: Set[String])(bootstrap: String): Unit =
    withAdminClientOn(bootstrap) { admin =>
      val nodeCount = Math.min(3, admin.describeCluster().nodes().get().size())

      topics
        .map(topicName =>
          new NewTopic(
            // this is the topic name
            topicName,
            // this is the number of partitions to set in the topic
            4,
            // this the number of replica
            nodeCount.toShort
          )
        )
        .foreach { topic =>
          Try {
            admin.createTopics(List(topic).asJava).all().get()
          }.fold(
            {
              case e: ExecutionException =>
                e.getCause match {
                  case _: TopicExistsException => ()
                  case e                       => throw e
                }
              case e => throw e
            },
            _ => ()
          )
        }

      retry(retryCount = 5, delayMs = 1000) {
        admin.listTopics(new ListTopicsOptions).names().get().asScala
      }(topics => topics.subsetOf(topics))
        .getOrElse(throw new IllegalStateException(s"unable to create topics: ${topics.mkString(", ")}"))

      println(s"Topics created: ${topics.mkString(", ")}")
    }

  def displayConsumerGroupOffset(bootstrap: String, group: String): Unit =
    withAdminClientOn(bootstrap) { admin =>
      val offsets: Map[TopicPartition, OffsetAndMetadata] =
        admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get().asScala.toMap

      println(s"Group $group offsets:")
      offsets.toList
        .sortBy { case (tp, _) => f"${tp.topic()}-${tp.partition()}%03d" }
        .map { case (tp, ofs) => s"  topic=${tp.topic()} - partition=${tp.partition()} - offset=${ofs.offset()}" }
        .foreach(println)
    }

  def consumerGroupToEarliest(bootstrap: String, group: String): Unit =
    withAdminClientOn(bootstrap) { admin =>
      val offsets: Map[TopicPartition, OffsetAndMetadata] =
        admin
          .listConsumerGroupOffsets(group)
          .partitionsToOffsetAndMetadata()
          .get()
          .asScala
          .toMap

      val topicPartitions: Map[TopicPartition, Long] =
        admin
          .listOffsets(
            offsets.keys
              .map(_ -> OffsetSpec.earliest())
              .toMap
              .asJava
          )
          .all()
          .get()
          .asScala
          .view
          .mapValues(_.offset())
          .toMap

      val newOffsets: util.Map[TopicPartition, OffsetAndMetadata] =
        topicPartitions.map { case (tp, offset) => tp -> new OffsetAndMetadata(offset) }.asJava

      admin.alterConsumerGroupOffsets(group, newOffsets).all().get()
    }

}
