# Introduction

## Goal

Your goal is to deliver and test an application that indicates the
current value of each stock available in a store.

This application enables the management of orders to suppliers (eg. if
the stock is too low) and to transmit up-to-date data of available
stock quantity to online clients of the store. In this purpose, the
application manages data from its delivery by the store, till the
request sent by the client services.

For that purpose, we will use a microservice architecture and see the
benefits of NoSQL technology to transport and store data between
services. We will also see that such architecture helps the application
to be fault-tolerant and scalable.

## Business rules

A store sends, to your information system, data about its stock all
along the day. The stock indicates for each product owned by the store,
the remaining quantity to sell at a given time.

Stock data are two-folds. There are:
* _observed stock_ indicating the new quantity of a product in the
  store.
* _delta_ indicating a variation in the stock (due to a sale or
  delivery) of a product.

Each time the application receives an _observed stock_ for a product,
the quantity _observed stock_ becomes the new reference quantity for
this product.

Each time the application receives a _delta_ for a product, the stored
quantity of this product is increased according to the quantity of the
_delta_ data.

## Constraints

* The application updates its data according to the received
  _observed stocks_ and _delta_. A sale generates a negative quantity
  and a delivery generates a positive quantity.
* The application is scalable.
* The application is still available even in case of partial failure.
