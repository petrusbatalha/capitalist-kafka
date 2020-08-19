# Capitalist Kafka
[![Build Status](https://travis-ci.org/joemccann/dillinger.svg?branch=master)](https://travis-ci.org/joemccann/dillinger)

We live in a capitalist world, so you should rely on The Capitalist to help you survive on this savage world. 

The Capitalist will help you to ensure that everything you produces will have a consumer to consume, so you will know if you're wasting resources and then you will be able to act !

The Capitalist, a cloud-enabled kafka lag exporter. Written in Rust to be blazingly fast ! 

## How it works?
- Consume the __consumer_offsets topic.
- For each consumed event it will fetch the topic highwatermark.
- Calculate the lag for the consumer group, topic and partitions !
- Expose metrics on the /metrics endopoint, using Prometheus format.

# License
###### - Capitalist hum? How much it cost?
###### - WAIT FOR IT ! IT'S FREE !
This project is licensed under the MIT License - see the LICENSE.md file for details

### Warning ! Is just a pet project, don't use in production.
