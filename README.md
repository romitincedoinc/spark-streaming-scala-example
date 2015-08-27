# kafka-spark-cassandra-example
This project provides a simple example of a Kafka / Spark Streaming / Cassandra stack.
This code has been implemented based on the Kafka and Cassandra Clusters that you can build yourself locally with the following project :

https://github.com/tquiviger/ansible-kafka-cassandra-cluster

## Prerequisites

#### Install SBT

For Mac, this can be done with Homebrew:

```
brew install sbt
```

## Usage

#### Creating tables in Cassandra

Assuming your Cassandra Cluster has been built up by * ansible-kafka-cassandra-cluster * , you'll have to ssh to one of the node with :

```
vagrant ssh cassandra-node-1

/etc/dsc-cassandra-2.2.0/bin/cqlsh cassandra-node-1
```

Then create a keyspace and the tables to persist your data.

```
CREATE KEYSPACE dev
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };
USE dev;

CREATE TABLE messages (
id int,
username text,
message text,
PRIMARY KEY (id));

CREATE TABLE message_count_by_user (
username text,
message_count counter,
PRIMARY KEY (username));
```

#### Launching the 2 main classes (see explanations further)

```
git clone git@github.com:tquiviger/spark-streaming-scala-example.git
cd spark-streaming-scala-example  

sbt compile
sbt "run-main GetAndSaveMessages"
##open new terminal tab
sbt "run-main GetAndSaveMessageCountByUser"
```

#### Creating topics in Kafka
```
vagrant ssh kafka-node-1
```
```
export LOG_DIR=/tmp # otherwise we get a warning
cd /etc/kafka_2.11-0.8.2.1/bin
./kafka-topics.sh --create --zookeeper zk-node-1:2181 --replication-factor 3 --partitions 1 --topic raw-messages
./kafka-topics.sh --create --zookeeper zk-node-1:2181 --replication-factor 3 --partitions 1 --topic userCount-messages
```
#### Producing messages in Kafka topics
```
./kafka-console-producer.sh --broker-list localhost:9092 --topic raw-messages
1-first message-user1
2-another message-user2
3-a message, again-user1
^C
```

#### Checking messages in Cassandra
ssh to whatever cassandra node you want and 

```
vagrant ssh cassandra-node-1

/etc/dsc-cassandra-2.2.0/bin/cqlsh cassandra-node-1
```

```
SELECT * FROM messages;

SELECT * FROM message_count_by_user;
```

## Main classes

These 2 jobs process messages from Kafka. The 1st one takes as an input messages with the format *messageid-message-username*

```
1234-my message-john
```

The second one takes as an input the messages produced by the 1st job, with the format *username-message_count*

```
john-12
```

#### GetAndSaveMessages

* connects to Kafka
* get all the messages from the topic **raw-messages**
* parse the messages
* save the message/username into Cassandra
* produce new message into the topic **userCount-messages** containing for every user the messages count for the last 30 seconds

#### GetAndSaveMessageCountByUser

* connects to Kafka
* get all the messages from the topic **userCount-messages**
* parse the messages
* save the message count for every username into Cassandra
