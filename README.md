# Real-Time DDOS Detector With Kafka+Spark
### Modified: November 9, 2020

## Introduction:
The following python project demonstrates a simple Kafka producer that uses an Apache web log output as the source. As the producer processes the incoming log text, it serializes it in a consumable JSON format and pushes to Kafka running on a Docker-provided broker. On the receiving end is a Spark consumer running on Docker-provided Spark master and worker. The Spark consumer uses Spark Streaming to process the messages in the http_logs topic. It counts the number of instances by IP, Agent, and OS. 

## Contents:
