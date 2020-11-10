# Real-Time DDOS Detector With Kafka+Spark
### Modified: November 10, 2020

## Introduction:
The following python project demonstrates a simple Kafka producer that uses an Apache web log output as the source. As the producer processes the incoming log text, it serializes it in a consumable JSON format and pushes to Kafka running on a Docker-provided broker. On the receiving end is a Spark consumer running on Docker-provided Spark master and worker. The Spark consumer uses Spark Structured Streaming to process the messages in the http_logs topic. It counts the number of instances by IP, Agent, and OS. 

## Contents:
The repository contains the following:
* Kafka + Spark Container
* Kakfa Producer
* Kafka Consumer w/ Spark Structured Streaming
* Spark Binaries and Libraries for Reference

## How It Works:
The first step is to create the required infrastructure. In this case, it uses Wurstmeister/kafka and bde2020/docker-spark. A modified docker-compose has the required elements for a single broker Kafka and Spark waster/worker. To build, run the following command after navigating to "/infra/kafka_spark": 

```bash
docker-compose up  
``` 

After the containers have been built, the kafka consumer and producer can be executed using:
```bash
python src/consumer.py
```
```
python src/producer.py
```

The producer will track resources/data-fill.txt and produce serialized messaages to Kafka topic "http_logs." The consumer based on Spark Structured Streaming subscribes to that topic and counts the number of requests made by user-agent. The top user-agent and associated hosts are then sent back to a Kafka topic (hosts) (for further analysis) and also outputted to a text file output.txt. The consumer will auto-terminate, the producer will need to be manually interrupted. The file "output.txt" contains a list of IPs associated with the highest-frequency user-agent. 

# Future Improvements:
This solution does not run on a cluster. While it's not necessary for this use case, additional partition and replication will help with performance. In addition, machine learning in the form of a logistic regression or other classifier could be used to better determine the attacker. Admittedly, the current method is not precise. 

Credit:
* Big Data Europe - Spark Infrastructure
* Wurstmeister - Kafka