package SparkIntegration

import org.apache.spark._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils



object kafka_WordCount {
  
   def main( args:Array[String] ){
     
    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaReceiver")
    
    val ssc = new StreamingContext(conf, Seconds(20))
    
    val kafkaStream = KafkaUtils.createStream(ssc, "localhost:2181","spark-streaming-consumer-group", Map("sample_topic" -> 5))
  
    //need to change the topic name and the port number accordingly
    val words = kafkaStream.flatMap(x =>  x._2.split(" "))
    
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    
    //prints the stream of data received
    kafkaStream.print() 
    
    //prints the word count result of the stream
    wordCounts.print()   
    
    ssc.start()
    ssc.awaitTermination()
  }
  
}