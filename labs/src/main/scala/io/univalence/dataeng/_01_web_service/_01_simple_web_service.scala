package io.univalence.dataeng._01_web_service

import spark.{Request, Response}
import spark.Spark._

/**
 * In this file, we will create our first Web service.
 *
 * ==Introduction==
 * A '''service''' is a specific kind application, that is not supposed
 * to be stopped, except for some specific cases:
 *
 *   - to upgrade the version of the service (to deliver new features or
 *     fixes)
 *   - in case of critical error, that can corrupt the information
 *     system of a company
 *   - to decommission an obsolete service
 *
 * A '''Web service''' is a service that uses the Web system and Web
 * standards to communicate.
 *
 * A '''Web framework''' is a set of libraries and tools that helps you
 * to develop Web services, and possibly to deploy them, communicate by
 * using HTTP, handling data, managing security... The Web is very
 * popular across the world. So, there are tons of Web frameworks like
 * [[https://rubyonrails.org/ Ruby On Rails]],
 * [[https://nodejs.org/ Node.js]] for JavaScript,
 * [[https://www.php.net/ PHP]],
 * [[https://github.com/azac/cobol-on-wheelchair Cobol On Wheelchair]]...
 *
 * ==Spark Web Framework==
 * The Java community also contains many Web frameworks. The current
 * most used one is
 * [[https://spring.io/projects/spring-boot Spring Boot]], when develop
 * in Java or Kotlin. There is also [[https://grails.org/ Grails]] (or
 * Groovy On Rails) for Groovy, [[https://www.playframework.com/ Play]]
 * for Java and Scala, [[https://http4s.org/]]... There is also a Web
 * framework in the JDK: [[com.sun.net.httpserver.HttpServer]].
 *
 * Here, we will use a simple Web framework, that does not require lots
 * of configurations and let's us focus on the exercises. This is
 * [[https://sparkjava.com/ Spark]] (not to be confused with
 * [[https://spark.apache.org/ Apache Spark]], the framework for
 * large-scala data analytics).
 */
object _01_simple_web_service {

  val serverPort = 8090

  def main(args: Array[String]): Unit = {

    /**
     * ==Initialization==
     * The first Web standard is the HTTP protocol. The HTTP protocol
     * uses a client/server communication, based in the TCP/IP stack.
     * This means that the Web service uses socket communication.
     *
     * So first, when we setup a Web server, you need to indicate the
     * host it has to listen to and the TCP/IP port for incoming
     * communications. Usually, we accept incoming communication from
     * any hosts. In this case, we use the address `0.0.0.0`. Note that
     * the framework Spark here automatically configure this address.
     *
     * Second, we have to indicate a port. The default port for a Web
     * service is `80`. But it is strongly possible that this port is
     * already in use by another Web service, like Apache httpd or
     * Nginx. `8080` is a usual fallback port. But still, there is a
     * strong possibility that a Web service like Tomcat or Jetty use
     * it. Spark suggests `4567` (see
     * [[spark.Service.SPARK_DEFAULT_PORT]]). But here, we will (try to)
     * ensure a different port for the different Web services we will
     * start from `8090`, and increase this number.
     *
     * BEWARE!!! Even if we have different port number for each Web
     * service, so they do not overlap, it is highly recommended that
     * you stop your Web services once they are not used anymore. If you
     * do that, you will overload your computer. You can stop a Web
     * service by hitting `&lt;Ctrl+C>`, if you are in a terminal, or by
     * clicking on the stop button in a IDE.
     */
    port(serverPort)

    /**
     * ==Route creation==
     * A '''route''' is like a rule: you associate a method (GET, POST,
     * DELETE...) and a path or a set of paths to a process.
     *
     * In the example below, when you run this application and you open
     * the URL `http://localhost:8090/hello` in a browser, you will a
     * JSON message.
     *
     * You can also use `curl` to open the URL. curl is a popular and
     * almost complete command-line tool to communicate data with URLs.
     * curl is available at https://curl.se/. It use to be available on
     * most Linux distributions and Unix derivatives. You may have to
     * download it and add it in your PATH for Windows. Here is how you
     * will use it:
     *
     * {{{
     *   $ curl http://localhost:8090/hello
     * }}}
     *
     * Note: there is another interesting command-line tool, especially
     * for JSON: this is `jq`. jq is a tool to query JSON documents and
     * provide a nice display. Here is an example:
     *
     * {{{
     *   $ curl http://localhost:8090/hello | jq
     * }}}
     *
     * You may see a progress bar. To avoid it, use the option
     * `--silent`.
     *
     * {{{
     *   $ curl --silent http://localhost:8090/hello | jq
     * }}}
     */
    get(
      // Path of the route (rightmost part of the URL).
      "/hello",
      // Associated process
      { (request: Request, response: Response) =>
        /**
         * By default, Spark sends HTML. So if we want the response to
         * be interpreted as JSON, we have to change the type.
         */
        response.`type`("application/json")

        /** The line below is what the client will receive. */
        """{"message": "hello"}"""
      }
    )

    /**
     * We can also describe a pattern in path, by prefixing the variable
     * part of the path by `:` and giving a name to this part.
     *
     *   - `http://localhost:8090/hello/jon`
     *   - `http://localhost:8090/hello/mary`
     *   - ...
     */
    get(
      "/hello/:name",
      { (request: Request, response: Response) =>
        response.`type`("application/json")

        /**
         * To use the variable part of the path, we simply call the
         * method `params` on `request`, with name use in the pattern.
         */
        val name = request.params("name")

        s"""{"message": "hello $name"}"""
      }
    )

    /**
     * ==Query==
     * In a URL, a query is a part where you can provide more data or
     * constraints. It appears at the rightmost of the query, after `?`,
     * as key-value data (in the form `key=value`) separated by `&`. For
     * example, `https://duckduckgo.com/?q=url&ia=web`.
     *
     * With the example of code below, you can try those URLs:
     *   - `http://localhost:8090/message?content=hello`
     *   - `http://localhost:8090/message?content=hello world`
     *   - `http://localhost:8090/message?content=hello+world`
     */
    get(
      "/message",
      { (request: Request, response: Response) =>
        response.`type`("application/json")

        /**
         * We use the `queryParams` method to get the value associated
         * to a key in the query. If the key does not exists, the method
         * returns `null`. In this case, we convert it into an empty
         * string.
         */
        val content =
          Option(
            request.queryParams("content")
          ).getOrElse("")

        s"""{"message": "$content"}"""
      }
    )

    /**
     * What to do when something goes wrong?
     *
     * The HTTP used to respond with the code 200 (OK). This code is
     * implicitly sent by the Web framework. But if something is not OK,
     * there are specific codes that you can send.
     *
     * One of the most known error on the Web is 404 (Not Found). This
     * error happens when you are trying a Web resource. In general, 4xx
     * errors caused by the client. You can also find 400 (Bad Request
     * —when request is malformed), 403 (Forbidden —when you do not have
     * the necessary permission for a resource), 408 (Request Timeout),
     * 418 (I'm a teapot —IETF April Fool's joke).
     *
     * 5xx are errors due to the Web server. 500 (Internal Server Error
     * —server cannot handle any request), 501 (Not Implemented —request
     * method not recognize by the server), 503 (Service Unavailable
     * —server cannot temporarily handle the request).
     *
     * The code below sends the code 404 when the client hit the path
     * `/wrong`.
     */
    get(
      "/wrong",
      { (request: Request, response: Response) =>
        response.`type`("application/json")
        response.status(404)

        s"""{"message": "Not Found"}"""
      }
    )
  }

}
