import java.sql.{Timestamp, Time}
import java.util.Properties

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector._
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import kafka.serializer.{StringDecoder, DefaultDecoder}
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils


object GetAndSaveMessages {
  def main(args: Array[String]): Unit = {

    //SPARK Configuration
    val sparkConf = new SparkConf()
      .setAppName("sparkMessages")
      .setMaster("local[2]")
      .set("spark.cassandra.connection.host", "cassandra-node-1")
    val sparkTimeInterval = new Duration(10000)
    val ssc = new StreamingContext(sparkConf, sparkTimeInterval)

    //CASSANDRA configuration
    val keyspaceName: String = "dev"
    val tableName: String = "messages"

    //KAFKA Configuration
    val brokers = "kafka-node-1:9092,kafka-node-2:9093,kafka-node-3:9094"
    val kafkaParams = Map(
      "zookeeper.connect" -> "zk-node-1:2181",
      "group.id" -> "spark-streaming-test",
      "zookeeper.connection.timeout.ms" -> "1000"
    )
    val inputKafkaTopic: String = "raw-messages"
    val outputKafkaTopic: String = "userCount-messages"
    val inputTopics = Map(inputKafkaTopic -> 1)
    val numDStreams = 2
    val numPartitions = 10

    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("producer.type", "sync")
    props.put("batch.num.messages", "3")
    props.put("message.send.max.retries", "3")
    props.put("request.required.acks", "-1")

    //Connecting to KAFKA DStream
    val stream =
      KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
        ssc,
        kafkaParams,
        inputTopics,
        storageLevel = StorageLevel.MEMORY_ONLY_SER // or: StorageLevel.MEMORY_AND_DISK_SER
      )

    //Processing DStream
    stream.foreachRDD(rdd => {
      rdd.flatMap(line => {
        val columns = line._2.split("-")
        val message_id = columns(0).toInt
        val message = columns(1)
        val username = columns(2)
        Some(message_id,message ,username)
      }).saveToCassandra(keyspaceName,tableName,SomeColumns("id", "message", "username" ))
      rdd.map(line =>(line._2.split("-")(2),1))
        .reduceByKey(_+_)
        .foreach(v => {
        val kafkaProducer = new Producer[String, String](new ProducerConfig(props))
        kafkaProducer.send(new KeyedMessage[String, String](outputKafkaTopic,v._1+"-"+v._2))
      }
      )
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
