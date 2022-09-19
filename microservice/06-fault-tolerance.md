# Tolerance to partial failure

The whole application is resilient to partial failure.

* If `ingest` fails: process service get the last messages from the
  topic and stores last new data in database. The api works as usual.
  The requests of the client will be served, but, at the end, they
  will not have the last updates, according to the available data.
* If `process` fails: ingest service continues to send data to the
  topic. Depending on the frequency of new data, the size of the data,
  and the storage capacity of the Kafka cluster, the cluster will be
  able to store data coming from ingest service a long time. This time
  should be enough for you to detect the failure and to restart the
  process service. api service works as usual, but with no data update.
* If `api` fails: the chain ingest-process continues to work as usual.
  But the clients will have no response to their requests.
* If `Kafka` fails: the services ingest and process fail also. api
  service works as usual, with no data update.
* If `database` fails: the services process and api fail also. ingest
  service works as usual, with storage capacity of the Kafka cluster.

#### => Exercise

* Simulate partial failure by stopping some services in the application
  and see what is happening.
* Does the whole application works as usual, if you only restart the
  stopped services (without restarting the other running services)?
