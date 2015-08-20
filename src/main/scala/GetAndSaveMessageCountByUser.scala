import com.datastax.spark.connector.{SomeColumns, _}

object GetAndSaveMessageCountByUser extends SparkStreamingJob {

  def main(args: Array[String]): Unit = {

    val inputKafkaTopics = Map("userCount-messages" -> 1)
    //Connecting to KAFKA DStream
    val stream =
      createKafkaStream(inputKafkaTopics)

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
      .saveToCassandra(keyspaceName, messageCountTableName, SomeColumns("username", "message_count"))
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
