
import com.datastax.spark.connector.{SomeColumns, _}
import kafka.producer.KeyedMessage


object GetAndSaveMessages extends SparkStreamingJob {

  def main(args: Array[String]): Unit = {

    val inputKafkaTopics = Map("raw-messages" -> 1)
    val outputKafkaTopic = "userCount-messages"

    //Connecting to KAFKA DStream
    val stream =
      createKafkaStream(inputKafkaTopics)

    //Processing DStream
    stream
      .foreachRDD(rdd => {
        rdd.flatMap(line => {
          val columns = line._2.split("-")
          val message_id = columns(0).toInt
          val message = columns(1)
          val username = columns(2)
          Some(message_id, message, username)
      }
      )
      //Saving parsed messages to Cassandra
      .saveToCassandra(keyspaceName, messageTableName, SomeColumns("id", "message", "username"))

      //Producing new messages to Kafka (username-messageCount)
      rdd.map(line =>
        (line._2.split("-")(2), 1))
        .reduceByKey(_ + _)
        .foreach(v => {
          kafkaProducer.send(new KeyedMessage[String, String](outputKafkaTopic, v._1 + "-" + v._2))
        }
        )
      }
      )

    ssc.start()
    ssc.awaitTermination()
  }
}
