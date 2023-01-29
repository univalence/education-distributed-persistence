# Deploy and run

## Docker

To run this application application, you will need to use Docker.

## Install Docker

If it is not done yet, download
[Docker Desktop](https://www.docker.com/products/docker-desktop/)
and follow instruction. Ensure then that the docker service/daemon
is running by launching Docker Desktop or by running this command:

```shell
$ docker info
```

It should display information about the Docker client and the
Docker server.

## Install Kafka & Cassandra

Both Apache Kafka and Apache Cassandra services come with predefined
Docker Compose files in this project, in the directory `docker/`.

### Run Apache Kafka

The Docker Compose file for Kafka spawns a cluster made of a Zookeeper
service (used by Kafka to managed its shared configuration and the
consensus) and 2 Kafka brokers.

Open a terminal and run

```shell
$ docker compose -f docker/docker-compose-kafka.yaml -p kafka up
```

There is mapping for Kafka for TCP port 9092 and 9093. 

### Run Cassandra

For Cassandra, you can or run a single node or 3 nodes, depending on
the performances of your computer.

It takes some times to launch Cassandra. Once ready, Cassandra is
available on port 9042.

#### Single Cassandra node

Open a new terminal and run

```shell
$ docker run -p 9042:9042 --name cassandra-db --rm cassandra:4
```

#### Multi Cassandra nodes

Open a new terminal and run

```shell
$ docker compose -f docker/docker-compose-cassandra.yaml -p cassandra up
```

### Prepare Kafka & Cassandra

Before running your application, you will need to initialize Kafka and
Cassandra, in order to create the necessary topic and table.

Scala: In the directory `demo`, open the file
`InitMain.scala` and run it.

### Run the applications

To run your application, this is what you will need

1. In `api`, run `MicroserviceApiMain`
2. In `process`, run `MicroserviceProcessMain`
3. In `ingest`, run `MicroserviceIngestMain`

#### => Exercise

In `demo` directory, run `InjectorMain`.

From your computer, in a browser or by using `curl` or `wget`, try
those URLs:

* http://localhost:8080/api/stocks/1
* http://localhost:8080/api/stocks/2
* http://localhost:8080/api/stocks/unknown
* http://localhost:8080/api/stocks

## Note on the Web

The World Wide Web (or simply the Web) is a system of resources
available over the Internet. It is based on HTTP protocol (_HyperText
Transfer Protocol_).

HTTP protocol works as a client/server protocol

1. A client connects to a server by using a TCP/IP socket.
2. The client sends an HTTP request to the server.
3. The server process the request and sends back an HTTP response.
4. The client get the response and disconnects from the server.

An HTTP request contains a method (or a verb), a path to the resource,
the protocol version, a list headers (they are metadata as key-value
pairs), and data (optional). Here is an example (`<CRLF>` is for the
characters _carriage return_ and _line feed_ or `\r\n`):

```text
GET /index.html HTTP/1.1 <CRLF>
Host: localhost <CRLF>
<CRLF>
```

The main methods are:

* `GET`: to get data from a Web resource.
* `POST`: create a new Web resource.
* `PUT`: to update a Web resource.
* `DELETE`: to remove a Web resource.

An HTTP response contains the protocol version, a status code of the
response, a status message, headers, and data (optional).

```text
HTTP/1.1 200 OK <CRLF>
Date: Tue, 6 Sep 2022 14:01:07 +0200 <CRLF>
Content-Type: plain/text <CRLF>
<CRLF>
OK
```

Web applications (ie. application that uses HTTP protocol to
communicate) are so common that it is easy to find libraries in
different languages to handle such protocol.

On server side:

* Spring boot, Java Spark for Java
* http4s, Akka http for Scala
* Play Framework for Java and Scala
* Grails for Groovy
* Ruby on Rails for Ruby
* Django, Flask for Python
* ...

On the client side:

* OkHttp for Java
* sttp, http4s for Scala
* ...

### How HTTP is used in our application?

The store sends data to the ingest service by using the HTTP protocol.
To do so, the store is seen as HTTP client. It connects to the ingest
service, seen as an HTTP server. Then the store sends POST requests
to the ingest service with data.

The api service is also an HTTP server. It waits for client to send
request on stored data about the available stocks.
