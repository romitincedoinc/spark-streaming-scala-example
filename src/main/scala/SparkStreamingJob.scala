import java.util.Properties

import kafka.producer.{ProducerConfig, Producer}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{StreamingContext, Duration}

class SparkStreamingJob {


  //SPARK Configuration
  val sparkConf = new SparkConf()
    .setAppName("sparkMessageCount")
    .setMaster("local[2]")
  sparkConf.set("spark.cassandra.connection.host", "cassandra-node-1")
  val sparkTimeInterval = new Duration(10000)
  val ssc = new StreamingContext(sparkConf, sparkTimeInterval)

  //CASSANDRA configuration
  val keyspaceName = "dev"
  val messageTableName = "messages"
  val messageCountTableName = "message_count_by_user"

  //KAFKA Configuration
  val brokers = "kafka-node-1:9092"
  val kafkaParams = Map(
    "zookeeper.connect" -> "zk-node-1:2181",
    "metadata.broker.list" -> brokers,
    "group.id" -> "spark-streaming-test",
    "zookeeper.connection.timeout.ms" -> "1000"
  )

  val props = new Properties()
  props.put("metadata.broker.list", brokers)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("producer.type", "sync")
  props.put("batch.num.messages", "3")
  props.put("message.send.max.retries", "3")
  props.put("request.required.acks", "-1")

  //Creating Kafka Producer
  lazy val kafkaProducer = new Producer[String, String](new ProducerConfig(props))


  def createKafkaStream(inputTopics: Map[String, Int]) = {
    KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc,
      kafkaParams,
      inputTopics,
      storageLevel = StorageLevel.MEMORY_ONLY_SER // or: StorageLevel.MEMORY_AND_DISK_SER
    )
  }

}
