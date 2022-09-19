# Deploy and run

## Target environment

To deploy and run the application, you will use a provided virtual
machine (VM). The name of the VM matches `s<id>.edu.univalence.io`.

To connect to those VMs, you will need a ssh connection (you must have
OpenSSH installed). To ensure that SSH is installed. This command
should show the SSH version: `ssh -V`.

## Install Kafka & Cassandra

### Install Apache Kafka

**Note**: Apache Kafka is already install on your VM. You can directly
to the next section.

* Install Kafka (download the TGZ file and uncompress it — for windows,
  ensure that the Kafka directory is closed to the disk root and that
  there is no space and no character with accent)

### Run Apache Kafka

Open a shell on the VM

```shell
$ ssh -P 2222 root@<host>
```

If asked to continue connecting, type `yes`.

Enter the password. Then, got to Kafka directory.

```shell
$ cd /opt/kafka
```

Launch Zookeeper:

```shell
$ ./bin/zookeeper-server-start.sh config/zookeeper.properties
```

Open a new terminal. Connect to your VM again with ssh and launch
Kafka:

```shell
$ ./bin/kafka-server-start.sh config/server.properties
```

In another terminal, you can add a second instance of Kafka:

```shell
$ ./bin/kafka-server-start.sh config/server.properties \
  --override broker.id=1 \
  --override log.dirs=/tmp/kafka-logs.1 \
  --override listeners=PLAINTEXT://:9093
```

Note for Windows: every scripts are under `bin\windows`. For example:

```shell
> bin\windows\zookeeper-server-start.bat config\zookeeper.properties
```

### Run Cassandra (with Docker)

**Note**: for Cassandra, you will not have to install anything. You
will only need docker, knowing that this tool is already available on
your VM.

Open a new terminal. Connect to your VM again with ssh and launch
Cassandra by using Docker:

```shell
$ docker run -p 9042:9042 --name cassandra-db --rm cassandra:4.0.3
```

### Test your environment

In the directory `/opt/test_vm_dp`, you have a tool to test your setup.
In this directory, you can simply run the following command

```shell
$ ./bin/test_vm_dp
```

It tests your Cassandra setup, then the Kafka setup, and at the end, it
launches a simple Web service.

Once the test tools is launched, you can query this URL by using your
Web browser.

* `http://<host>:8080/api`

It must send you back a JSON message.

To stop the test Web service, open this URL below.

* `http://<host>:8080/stop`

## Deploy your application

* Open `sbt shell` at the bottom of IntelliJ IDEA.
* Run `Universal/packageBin`

This has compiled code and generated Zip files of the different
projects in the directory `microservice`.

Repeat the process below for each of the following sub-projects in
`microservice` directory: `api`, `demo`, `ìngest`, `process`.

* Go to the directory of the sub-project, and then go to the directory
  `target/universal`. You should find the Zip file at this level.
* Copy the Zip file to your VM with `scp`.

```shell
$ scp -P 2222 target/universal/<file>.zip root@<host>:./
```

* Unzip the copied file in the remote VM.

```shell
$ unzip <file>.zip
```

### Prepare Kafka & Cassandra

Before running your application, you will need to initialize Kafka and
Cassandra, in order to create the necessary topic and table.

From `microservice-demo` directory, run `./bin/init-main`.

### Run the applications

To run your application, this is what you will need

1. In `microservice-api`, run `./bin/microservice-api`
2. In `microservice-process`, run `./bin/microservice-process`
3. In `microservice-ingest`, run `./bin/microservice-ingest`

#### => Exercise

In `microservice-demo` directory, run `./bin/injector-main`.

From your computer, in a browser or by using `curl` or `wget`, try one
of those URLs:

* http://&lt;host>:8080/api/stocks/1
* http://&lt;host>:8080/api/stocks/2
* http://&lt;host>:8080/api/stocks/unknown
* http://&lt;host>:8080/api/stocks

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
