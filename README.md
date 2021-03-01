# Capitalist Kafka
[![Build Status](https://travis-ci.org/joemccann/dillinger.svg?branch=master)](https://travis-ci.org/joemccann/dillinger)

We live in a capitalist world, so you should rely on The Capitalist to help you survive on this savage world. 

The Capitalist will help you to ensure that everything you produces will have a consumer to consume, so you will know if you're wasting resources and then you will be able to act !

The Capitalist, a cloud-enabled kafka lag exporter. Written in Rust to be blazingly fast ! 

## How it works?
- Consume the __consumer_offsets topic.
- Parse de kafka message to readable text and store in RocksDB.
- Expose 3 routes. 
- /groups - shows all the kafka groups and its states.
- /topics - shows all kafka topics.
- /lag/{group} - shows lag for the specified group.

# License
This project is licensed under the MIT License - see the LICENSE.md file for details

### Warning ! Is just a pet project, don't use in production.
