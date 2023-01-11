package io.univalence.dataeng._04_cassandra_db

import com.datastax.oss.driver.api.core.CqlSession

import io.univalence.dataeng.internal.cassandra_utils._
import io.univalence.dataeng.internal.exercise_tools._

import scala.util.Using

import java.net.InetSocketAddress

/**
 * =Cassandra=
 * Cassandra is a column-oriented NoSQL database with eventual
 * consistency (meaning that it is focused on Availability and Partition
 * tolerance, following the CAP theorem).
 *
 * Cassandra is designed to handle a high volume of data by scaling in
 * using multiple nodes with no single point of failure (SPOF). Thus, it
 * is designed as a multi master system and not a master/slave system.
 * It means that some nodes can die without impacting directly the
 * system. So, operators have time replace dead nodes.
 *
 * Cassandra has first been created in Facebook offices and is now an
 * open source project under the Apache foundation, mainly managed by
 * the company Datastax. Cassandra is written in Java.
 *
 * ==Concepts==
 * Cassandra is built around different concepts:
 *   - '''Keyspace''': Configurations containing tables and defining how
 *     many times we should replicate the data for example.
 *   - '''Table''': Defines the typed schema for a collection of
 *     partitions.
 *   - '''Partition''': Defines the mandatory part of the primary key
 *     all rows in Cassandra must have to identify the node in a cluster
 *     where the row is stored.
 *   - '''Row''': Collection of columns identified by a primary key.
 *   - '''Column''': A single column with a type for example (name:
 *     String). It is possible to use composite types like lists, maps,
 *     sets, and any kind of sub-structures.
 *
 * ==Data storage==
 * Cassandra also uses the same type of data storage as RocksDB. It is
 * also based on LSM-tree, which enhances huge amount of writes in the
 * database.
 *
 * ==Cassandra specificity==
 * Cassandra differs a lot from SQL database, because it is
 * [[https://cassandra.apache.org/doc/latest/cassandra/data_modeling/data_modeling_rdbms.html#query-first-design query first]].
 * It means that we design Cassandra table with queries in mind. Indeed
 * in Cassandra you cannot join tables and you can query the data only
 * using the partition keys. If you need to query the same kind of data
 * in two different manners, then you denormalize the data and you
 * create two tables with different partition key.
 *
 * Also, even if you can use ORDER BY to order your data, it is advised
 * to use your clustering key to sort your data.
 *
 * ==In this file==
 *   - The code is wrapped around a TestContainer creating the Cassandra
 *     container to interact with.
 *   - You then have an exercise to interact with Cassandra.
 */
object _01_basic_operations {

  import scala.jdk.CollectionConverters._

  /**
   * To make this program work, you have first to use Docker.
   *
   * ==Install and running Docker==
   * If it is not done yet, download
   * [[https://www.docker.com/products/docker-desktop/ Docker Desktop]]
   * and follow instruction. Ensure then that the docker service/daemon
   * is running by launching Docker Desktop or by running this command:
   *
   * {{{
   *   docker info
   * }}}
   *
   * It should display information about the Docker client and the
   * Docker server.
   *
   * ==Running Cassandra==
   * There are 2 ways to run a Cassandra: standalone mode (1 node only)
   * or cluster mode (3 nodes). You have to choose between those 2
   * modes. Prefer to use the cluster mode. But, if you have not enough
   * resources on your computer (eg. not enough CPU cores, or memory),
   * you will have to fallback to the standalone mode.
   *
   * ===Cluster mode===
   * A docker compose file is available in the directory `docker/` in
   * this project. It spawns 3 nodes
   * {{{
   *   docker-compose -f docker/docker-compose-cassandra.yaml up -d
   * }}}
   *
   * To stop the cluster, simply us this command:
   * {{{
   *   docker-compose -f docker/docker-compose-cassandra.yaml down
   * }}}
   *
   * ===Standalone mode===
   * Run the following command in a terminal to start Cassandra
   * {{{
   *     docker run --name cassandra -p 9042:9042 -d cassandra:latest
   * }}}
   *
   * (Optionally) You run the Cassandra shell, to check the setup works
   * well
   * {{{
   *     docker exec -it cassandra cqlsh
   * }}}
   *
   * Note: to stop Cassandra, you have to run this command
   * {{{
   *     docker stop cassandra
   * }}}
   */
  def main(args: Array[String]): Unit =
    Using(
      // connect to Cassandra database
      CqlSession
        .builder()
        .addContactPoint(new InetSocketAddress("localhost", 9042))
        .withLocalDatacenter("datacenter1")
        .build()
    ) { session =>
      exercise("Check the cluster") {

        /**
         * First, get information about the local Cassandra node (the
         * one we are connected on). Those information are available in
         * the table `system.local`.
         */
        val localResult = session.execute("""SELECT * FROM system.local""")
        val localRows   = localResult.all().asScala.toList
        displayRows(localRows)

        if (localRows.size == 1 && localRows.head.getString("bootstrapped") == "COMPLETED")
          comment("Cassandra is ready")
        else
          comment("Cassandra is not ready")

        check(localRows.size == 1)
        check(localRows.head.getString("bootstrapped") == "COMPLETED")

        val peersResult = session.execute("""SELECT * FROM system.peers""")
        val peersRows   = peersResult.all().asScala.toList
        displayRows(peersRows)

        comment(s"Cassandra cluster has ${peersRows.size + 1} node(s)")
      }

      exercise_ignore("Create a keyspace") {

        /**
         * This CQL query will create a keyspace.
         *
         * TODO modify this query to create the keyspace `univalence`
         *
         * Note: once done, ensures that this exercise is ignored again.
         */
        session.execute("""CREATE KEYSPACE IF NOT EXISTS ??? WITH replication = {
                          |  'class':              'SimpleStrategy',
                          |  'replication_factor': '1'
                          |}""".stripMargin)
      }

      exercise_ignore("Create a table") {

        /**
         * The CQL query below will create a table.
         *
         * TODO Create a table named `user` to store user information
         * with their id, name, and age.
         *
         * Note: once done, ensures that this exercise is ignored again.
         */
        session.execute("""CREATE TABLE IF NOT EXISTS ??? (
                          |  ???
                          |)""".stripMargin)
      }

      exercise_ignore("Add data") {

        /**
         * To add data in CQL is the same has with SQL. In the case of
         * CQL you can between the usual SQL syntax for INSERT, or you
         * can use another version based on JSON.
         */
        session.execute(
          """INSERT INTO univalence.user JSON '{  "id": "123",  "name": "jon",  "age": 32 }'"""
        )

        /**
         * Note: once done, ensures that this exercise is ignored again.
         *
         * TODO Add more records in the table, by using INSERT
         * statement, for those users:
         */
        /*
         * Emma-Sophie,15
         * Maria,28
         * Mario,39
         * Elena,31
         * Andrew,64
         * Panagiotis,66
         * Anastasios,39
         * Pierre,77
         * Logan,58
         * George,62
         * Logan,50
         * Elise,91
         * Alan,22
         * Dimitrios,38
         * Georgios,14
         */
      }

      exercise_ignore("Query data") {

        /**
         * TODO Use a SELECT statement to get all users from the table
         */
        val result = session.execute("""SELECT id, name, age FROM ???""")

        println("List of all users")
        display(result)
      }

      exercise_ignore("Query data as JSON document") {

        /**
         * TODO Use a SELECT statement to get all users from the table
         * in JSON format
         *
         * Note: it is a good idea to LIMIT the number of records in
         * output, while doing data exploration.
         */
        val result = session.execute("""SELECT JSON id, name, age FROM ??? LIMIT 100""")

        println("List of all users (JSON)")
        display(result)

      }

      exercise_ignore("Query with constraint") {

        /** TODO make this query return the user with id '123' */
        val result = session.execute("""SELECT id, name, age FROM ??? WHERE ???""")
        val rows   = result.all().asScala.toList

        println("Users 123")
        displayRows(rows)
        check(rows.size == 1)
        check(rows.head.getString("id") == "123")
      }

      exercise_ignore("Use prepared statement") {

        /**
         * Sometimes, you need to use query that depends on external
         * parameters.
         *
         * In the example below, we have created a function that tries
         * to find a user from its ID. But, we want the function to stay
         * generic enough to work with any IDs.
         *
         * The problem is that the ID parameter is a string. So, it can
         * accept any kind of string: an ID, "hello", several copies of
         * the complete works of Shakespeare in Mandarin (leading to a
         * deny of service attack), or worse, some SQL injection attack.
         *
         * [[https://en.wikipedia.org/wiki/SQL_injection SQL injection]]
         * consists in injecting an input as a string that tries to hack
         * your SQL statement ([[https://xkcd.com/327/]]). To guard
         * against this attack, it is better to use ''prepared
         * statements'', that checks for SQL injection and put the
         * result in a cache.
         *
         * A prepared statement is an incomplete CQL query, where `?` is
         * used as a placeholder in the WHERE clause.
         *
         * {{{
         *   val query = "SELECT * FROM table WHERE name = ?"
         * }}}
         *
         * The placeholder is bound to a value by calling the `bind`
         * method on the prepared statement.
         *
         * {{{
         *   val statement = session.prepare(query).bind("Edgard")
         * }}}
         */

        def findUserById(id: String): User = {

          /**
           * TODO Complete the query below so it returns a user
           * according to the given ID.
           */
          val statement = session.prepare("""SELECT id, name, age FROM ??? WHERE ???""")
          val result    = session.execute(statement.bind(id))
          val rows      = result.all().asScala.toList

          check(rows.size == 1)
          val row = rows.head

          // convert row into a user
          User(row.getString("id"), row.getString("name"), row.getInt("age"))
        }

        println("User 123")
        val user = findUserById("123")
        check(user == User("123", "jon", 32))
      }

      exercise_ignore("Find many users") {

        def findUsersByIds(ids: List[String]): List[User] = {

          /**
           * TODO Complete the query below so it returns a user
           * according to the given ID.
           */
          val statement = session.prepare("""SELECT id, name, age FROM ??? WHERE ???""")
          val result    = session.execute(statement.bind(ids.asJava))
          val rows      = result.all().asScala.toList

          rows.map(row =>
            // convert row into a user
            User(row.getString("id"), row.getString("name"), row.getInt("age"))
          )
        }

        val result = findUsersByIds(List("123", "456"))

        check(
          result == List(
            User("123", "jon", 32),
            User("456", "mary", 25)
          )
        )
      }

      exercise_ignore("Query with constraint on non-key field") {

        /**
         * TODO Try to get users whose age is greater or equal to 30.
         */
        val result = session.execute("""SELECT id, name, age FROM univalence.user WHERE ???""")

        /**
         * So you have a error message...
         *
         * Look to the error closely and try to solve the problem.
         *
         * TODO try to guess why constraints on non-key field is not
         * allowed by default?
         */

        println("Users greater or equal to 30")
        display(result)

        /**
         * It is preferable to get all records and filter afterwards in
         * your application, than to filter records in the query on a
         * non-key field.
         */
      }
    }.get

  case class User(id: String, name: String, age: Int)

}
