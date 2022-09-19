# Scalability

The application is scalable: by adding more instances of each service,
you can increase the performance of the application.

* If you add more instances of `process` services: the reason to do
  this is that you have more data to process from the Kafka topic. This
  means that the incoming data traffic at ingest service has increased,
  that you have increased the count of instances for ingest service and
  also that you have extended the Kafka cluster. It might be necessary
  to extend the Cassandra cluster.
* If you add more `ingest` services: the reason to do this is that have
  more incoming data. But if you add more ingest service, the client,
  who sends data, will need to deal with more addresses (ie. set of
  pairs host:port). In a view to expose only one address to such
  client, you can add a reverse proxy in front of ingest services.
  A _reverse proxy_ exposes only one address to an Internet client and
  can redistribute the client request to a set of services. In
  addition, this redistribution can be done by according to the load of
  each service. This is named _load balancing_. As an example,
  [Nginx](https://nginx.org/) and [HAProxy](http://www.haproxy.org/)
  provide reverse proxy and load balancing. In addition to all this,
  extending the Kafka cluster might be necessary. This is not the case
  for the process service.
* If you add more instances of `api` service, the application will be
  able to respond to more client requests. But to avoid the client to
  deal with more addresses, once again a reverse proxy will be
  necessary. More api instances also means more requests to the
  database. You should then extend the cluster of your database if it
  can scale (this is the case for Cassandra).

#### => Exercise

* Try to add instances of the process service (just run the service
  again). Does it receive messages from Kafka?
* Remove one of the instance of the process service. Do you lose
  data?
