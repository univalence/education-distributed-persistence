package io.univalence.dataeng._05_elasticsearch

import io.univalence.dataeng.internal.elasticsearch_utils._
import io.univalence.dataeng.internal.exercise_tools._

/**
 * =Elasticsearch=
 *
 * [[https://www.elastic.co/elasticsearch/ Elasticsearch]] is a
 * distributed search engine written in Java.
 *
 * A '''search engine''' is a software able to retrieve documents from a
 * database, according to given criteria. Elasticsearch retrieves
 * documents based on exact keywords, partial keywords, proximity to
 * keywords, and value range. In the case of partial search, keywords
 * are represented with wildcard (eg. "chatG*") or can be retrieve by
 * using synonyms, phonetic proximity (with a plugin),
 * [[https://en.wikipedia.org/wiki/Levenshtein_distance Levenshtein distance]],
 * geographical proximity...
 *
 * Elasticsearch database is a distributed document store and uses JSON
 * to represent documents. Every document are indexed and stored in a
 * table-like structure, named an '''index'''.
 *
 * ==Run Elasticsearch==
 *
 * To run Elasticsearch and to do the exercises below, you will need tu
 * use Docker.
 *
 *   - If it is not done yet, download
 *     [[https://www.docker.com/products/docker-desktop/ Docker Desktop]]
 *     and follow instructions.
 *   - Run the following command in a terminal to start Elasticsearch
 *     {{{
 *   docker run -p 9200:9200 -p 9300:9300 \
 *     --name elasticsearch \
 *     -e "discovery.type=single-node" \
 *     -e "xpack.security.enabled=false" \
 *     -e "xpack.security.enrollment.enabled=false" \
 *     docker.elastic.co/elasticsearch/elasticsearch:8.5.3
 *     }}}
 *
 * ==Exercices in this file==
 * All the exercises are ignored in this file are ignored. Convert each
 * `exercise_ignored` to `exercise` (ie. activate each exercise) one by
 * one, run the file, see what has to be solved or simply observe the
 * output. Once an exercise is done, toggle it into `exercise_ignored`.
 */
object _01_introduction {

  def main(args: Array[String]): Unit =
    section("Discovering Elasticsearch") {

      /**
       * There are different way to access Elasticsearch. You can use a
       * driver specific to the language you are using. But, you also
       * can use the REST API.
       *
       * This is what we will do in this exercise. The base URL is
       * below.
       */

      val baseUrl = "http://localhost:9200"

      exercise_ignore("Check communication with ElasticSearch") {

        /**
         * To communicate with Elasticsearch by using the REST API needs
         * you to know the
         * [[https://en.wikipedia.org/wiki/Hypertext_Transfer_Protocol HTTP protocol]].
         *
         * ==HTTP==
         * This protocol was first dedicated to communicate over the
         * Web. But, it can be used to ensure communication between
         * applications. HTTP is based on an "non-connected"
         * client-server approach:
         *   1. A client application connects to a server application.
         *   1. The client sends a request to the server.
         *   1. The server processes the request.
         *   1. The server sends a response to the client.
         *   1. Once client received the response, it disconnects from
         *      the server.
         *
         * In our case, the client application is represented by this
         * code and the server application is Elasticsearch.
         *
         * ===Request===
         * The request sent by the client contains:
         *   - a method (or verb): GET, POST, DELETE...
         *   - a path (and the query): `/user/_create/123`
         *   - the protocol version
         *   - headers (or metadata)
         *   - a content (named body, optional)—they are data sent to
         *     the server
         *
         * The server and the path used to be determined by an HTTP
         * [[https://en.wikipedia.org/wiki/URL URL]] (Universal Resource
         * Locator). Its syntax is
         * {{{
         *   scheme "://" host [":" port] "/" path ["?" query]
         * }}}
         *
         * where scheme used to be `http` or `https`, query is a set of
         * key-value pair separated by `&` character
         * (`q=mar*&fuzziness=true`).
         *
         * Each method in the request has a meaning:
         *   - GET: get a resource (eg. a document) from the server
         *   - POST: send a resource to the server
         *   - PUT: update a resource on the server
         *   - DELETE: remove a resource from the server
         *
         * ===Response===
         * The response sent by the server contains:
         *   - the protocol version
         *   - a status code: 200, 404, 503...
         *   - a status message: OK, Not Found,
         *   - headers (or metadata)
         *   - a content (named body, optional)—they are data sent to
         *     the client
         *
         * Each status code/message comes with a meaning:
         *   - 1xx: information
         *   - 2xx: success
         *     - 200 OK: the process of the request ends successfully
         *     - 201 Created: the sent resource as successfully been
         *       created
         *   - 3xx: redirection
         *   - 4xx: client errors
         *     - 400 Bad Request: the server detected an error in the
         *       request sent by the client
         *     - 404 Not Found: the resource requested by the client
         *       could not be found
         *   - 5xx: server errors
         *     - 500 Internal Server Error: sent by the server when it
         *       fails and has no suitable message
         *     - 503 Service Unavailable: The server temporarily cannot
         *       respond (because it is initializing or it is
         *       overloaded)
         *
         * ==HTTP calls in this lab==
         * In this exercise, we provide functions like [[http_GET]],
         * [[http_POST]] to represent and send HTTP requests to a
         * server. Those methods takes at least a
         * [[https://en.wikipedia.org/wiki/URL URL]] as parameter.
         *
         * Below, we only send a GET request on the base URL, and then
         * check if the server answers with OK (200), while displaying
         * its response.
         */

        val response = http_GET(baseUrl)

        // display the full pretty-printed response on screen
        display(response)

        // check the HTTP response status
        check(response.code == 200)
        check(response.message == "OK")
      }

      exercise_ignore("Create an index for users") {

        /**
         * In this exercise, we start by creating an index of users. To
         * create an index, you only need to send a document.
         *
         * If you want to store the user `(id=123, name=jon, age=35)`,
         * you have to send a POST request with those data in
         * [[https://en.wikipedia.org/wiki/JSON JSON format]] (see the
         * [[https://www.json.org/json-en.html syntax]]) to the server
         * and indicate in the path of the URL, the index name (`user`),
         * the operation (`_create`), and the key (`123`).
         *
         * It is also possible to create an empty index. You will need
         * to send then a PUT request.
         *
         * Execute this request and check the server response.
         */

        val response =
          http_POST(
            s"$baseUrl/user/_create/123",
            """{
              |  "id": "123",
              |  "name": "jon",
              |  "age": 35
              |}""".stripMargin
          )

        display(response)

        check(response.code == 201)
        check(response.message == "Created")

        if (response.code == 201) {

          /**
           * To explore JSON document, we have added some operations:
           *   - `json / "field"` extracts a field inside a JSON object
           *   - `json(n)` extracts a cell inside a JSON array
           *   - `json.getAs<type>` forces the conversion to Java type
           *
           * Suppose that you have this document in the varaible
           * `jsonDoc`:
           * {{{
           *     {
           *       "a": [
           *         {"b": 24},
           *         {"c": "bob"}
           *       ]
           *     }
           * }}}
           *
           * Thus
           *   - `((jsonDoc / "a")(0) / "b").getAsInt` gives 24
           *   - `((jsonDoc / "a")(1) / "c").getAsString` gives `"bob"`
           */
          check((response.bodyAsJson / "_shards" / "failed").getAsInt == 0)
        }

        /**
         * TODO try to execute the request above once again. What
         * happens?
         *
         * Once you have executed the request again, if you want to
         * create the user again, you will need to delete it first from
         * the user index. To do so, put this line before the sent of
         * POST request above.
         *
         * {{{
         *   http_DELETE(s"$baseUrl/user/_doc/123")
         * }}}
         */
      }

      exercise_ignore("Add more users and count them") {

        /**
         * TODO add those users in the user index
         *
         * Note: You can generate your own ID values
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

        /**
         * We can ask Elasticsearch for the number of stored document in
         * an index.
         */

        val response = http_GET(s"$baseUrl/user/_count")

        display(response)

        check((response.bodyAsJson / "count").getAsInt == 16)
      }

      exercise_ignore("Get a user by its ID") {

        /** Now, we will retrieve a user according to */

        val response = http_GET(s"$baseUrl/user/_doc/123")
        display(response)

        check(response.code == 200)
        check(response.message == "OK")
        check((response.bodyAsJson / "found").getAsBoolean == true)

        val data = response.bodyAsJson / "_source"
        check((data / "id").getAsString == "123")
        check((data / "name").getAsString == "jon")
        check((data / "age").getAsInt == 35)
      }

      exercise("Get a user by name") {

        /**
         * As a search engine, Elasticsearch is able to retrieve
         * documents based on criteria on fields that are not a key.
         * Elasticsearch proposes many approaches to search document. We
         * will use two of them.
         *
         * All those document retrieval approaches uses the search API
         * of Elasticsearch. They use
         * [[https://lucene.apache.org/ Apache Lucene]].
         *
         * A first approach is to retrieve document based on combination
         * of boolean clauses. The boolean clauses are:
         *   - `must`: the clause must appear in the document
         *   - `filter`: the clause must appear in the document
         *   - `should`: the clause should appear in the document
         *   - `must_not`: the clause must not appear in the document
         */

        val response =
          http_POST(
            s"$baseUrl/user/_search",
            """{
              |  "query": {
              |    "bool": {
              |      "must": {
              |        "term": { "name": "jon" }
              |      }
              |    }
              |  }
              |}""".stripMargin
          )

        display(response)

        val data = (response.bodyAsJson / "hits" / "hits")(0) / "_source"
        check((data / "id").getAsString == "123")
        check((data / "name").getAsString == "jon")
        check((data / "age").getAsInt == 35)
      }

      exercise_ignore("Retrieve the user Maria") {
        val response =
          http_POST(
            s"$baseUrl/user/_search",
            """{???}""".stripMargin
          )

        display(response)

        val data = (response.bodyAsJson / "hits" / "hits")(0) / "_source"
        check((data / "name").getAsString == "Maria")
        check((data / "age").getAsInt == 28)
      }

      exercise("Search users with a query string") {

        /**
         * Another way to retrieve document is to use a query string.
         *
         * A query string allows to use both advanced and simplified
         * queries. Here are examples of what you can find in a query
         * string.
         *   - `*` or `*:*`: any value in any field
         *   - `name:*`: any value for the field `name`
         *   - `name:ma*`: the field `name` must contain a substring
         *     that starts with `ma`
         *   - `abc`: the value `abc` in any field
         *   - `abc def` or `abc OR def`: any field must contain `abc`
         *     or `def`
         *   - `abc AND def`: any field must contain `abc` and `def`
         *   - `name:abc`: the field `name` must contain `abc`
         *   - `name:"John Smith"`: the field `name` must contain
         *     exactly `John Smith`
         *   - `name:John Smith`: the field `name` must contain `John`
         *     or any field should `Smith`
         *   - `name:John name:Smith`: the field `name` must contain
         *     `John` or `Smith`
         *   - `jphn~`: any field must contains a substring that matched
         *     `jphn` according an edit distance
         *   - `id:/[a-z]{3}[0-9]{3}/`: the field `id` must match the
         *     regular expression `[a-z]{3}[0-9]{3}` (eg. abc123)
         *   - `abc +def`: any value should contain `abc` and must
         *     contain `def`
         *   - `abc -def`: any value should contain `abc` and must not
         *     contain `def`
         *   - `age:[10 TO 20]` or `age:(>=10 AND <=20)`: the field
         *     `age` must contain a value between 10 and 20 (inclusive)
         *   - `age:[* TO 20]` or `age:<=20`
         *   - `age:[10 TO *]` or `age:>=10`
         *   - `age:{10 TO 20}` or `age:(>10 AND <20)`
         *   - `age:[10 TO 20}` or `age:(>=10 AND <20)`
         *
         * Note that you can provide a query string with a GET request,
         * by using the query part of the URL (syntax:
         * &lt;base-url>/&lt;index>/_search?q=&lt;query-string>).
         */
        val response =
          http_POST(
            s"$baseUrl/user/_search",
            """{
              |  "query": {
              |    "query_string": {
              |      "query": "name:mar*"
              |    }
              |  }
              |}""".stripMargin
          )

        display(response)

        val data =
          (response.bodyAsJson / "hits" / "hits").toList
            .map(j => (j / "_source" / "name").getAsString)
            .toSet
        check(data == Set("Maria", "Mario"))
      }

      exercise("Write a search request to get all users between 20 and 30") {
        val response =
          http_POST(
            s"$baseUrl/user/_search",
            """{???}""".stripMargin
          )

        display(response)

        val data =
          (response.bodyAsJson / "hits" / "hits").toList
            .map(j => (j / "_source" / "name").getAsString)
            .toSet
        check(data == Set("Maria", "Alan"))
      }
    }

}
