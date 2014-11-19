package test

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._

object FileStreaming {
  val WINDOW_LENGTH = new Duration(30 * 1000)
  val SLIDE_INTERVAL = new Duration(10 * 1000)

  def main(arg: Array[String]) {
    
    println("program started")
    
    val sc = new SparkContext("local","file streaming example")
    val ssc = new StreamingContext(sc,Seconds(1))
    val tfs = ssc.textFileStream("/home/HadoopUser/Downloads/target")
    
    val accessLogsDStream = tfs.map(x=>x.split(" ")).cache()
    val windowDStream = accessLogsDStream.window(WINDOW_LENGTH, SLIDE_INTERVAL)

    windowDStream.foreachRDD(accessLogs => {
      if (accessLogs.count() == 0) {
        println("No input received in this time interval")
      } else {
        // Calculate statistics based on the content size.
        
        accessLogs.foreach(x=> x.foreach(println))
              }
    })

    
    
    
    
    
    ssc.start()
    ssc.awaitTermination()
  }
  
  
}