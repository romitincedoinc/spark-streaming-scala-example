import com.datastax.spark.connector.{SomeColumns, _}
import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, StreamingContext}

object GetAndSaveMessageCountByUser {
  def main(args: Array[String]): Unit = {

    //SPARK Configuration
    val sparkConf = new SparkConf()
      .setAppName("sparkMessageCount")
      .setMaster("local[2]")
      .set("spark.cassandra.connection.host", "cassandra-node-1")
    val sparkTimeInterval = new Duration(10000)
    val ssc = new StreamingContext(sparkConf, sparkTimeInterval)

    //CASSANDRA configuration
    val keyspaceName: String = "dev"
    val tableName: String = "message_count_by_user"

    //KAFKA Configuration
    val brokers = "kafka-node-1:9092,kafka-node-2:9093,kafka-node-3:9094"
    val kafkaParams = Map(
      "zookeeper.connect" -> "zk-node-1:2181",
      "group.id" -> "spark-streaming-test",
      "zookeeper.connection.timeout.ms" -> "1000"
    )
    val inputKafkaTopic: String = "userCount-messages"
    val inputTopics = Map(inputKafkaTopic -> 1)
    val numDStreams = 2
    val numPartitions = 10

    //Connecting to KAFKA DStream
    val stream =
      KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
        ssc,
        kafkaParams,
        inputTopics,
        storageLevel = StorageLevel.MEMORY_ONLY_SER // or: StorageLevel.MEMORY_AND_DISK_SER
      )

    //Processing DStream
    stream
      .foreachRDD(rdd => {
      rdd.flatMap(line => {
        val columns = line._2.split("-")
        val username = columns(0)
        val currentCount = columns(1).toInt
        Some(username, currentCount)
      }
      )
      .saveToCassandra(keyspaceName, tableName, SomeColumns("username", "message_count"))
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
