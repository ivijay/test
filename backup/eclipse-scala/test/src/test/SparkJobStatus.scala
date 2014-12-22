package test

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.scheduler._
//imoprt org.apache.

object SparkJobStatus {
  def main(args: Array[String]) {
  println("start of spark job")
  val sparkConf = new SparkConf().setAppName("Tweet Analyzer").setMaster("local");
  val sc = new SparkContext(sparkConf);
  sc.addSparkListener(new SparkListener {
  override def onStageCompleted(event: SparkListenerStageCompleted) = {
    val myStage = "Tweet Analyzer"
    if (event.stageInfo.stageId == 0) {
      println("Stage $myStage is done.")
    }
//    println(event.stageInfo.stageId)
  }
})
  val file = sc.textFile("/home/HadoopUser/Downloads/input.txt");
  val countsPair = file.flatMap(line => line.split(" ")).map(word => (word,1)).foreach(println)
//                 counts.saveAsTextFile("test") 
}
}