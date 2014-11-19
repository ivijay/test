
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object LogAnalyzer {
  
  def main(args: Array[String]) {
    
    println("start of program")
    val sc = new SparkContext("local","log analyzer")
    
    val accessLogs = sc.textFile("/home/HadoopUser/Downloads/log-file2.txt").map(ApacheAccessLog.parseLogLine).cache
    println (accessLogs.getClass )
//    accessLogs.foreach(println)
    
    val urls = accessLogs.map(log => log.endpoint)
//    urls.foreach(println)
    
    val urlsCount = urls.map(x=>(x,1)).reduceByKey(_+_)
    
//    val top10 = urlsCount.top(10)(OrderingUtils.SecondValueOrdering)
    
//    val top10 = urlsCount.sortBy(x=> x._2, false )
    val top10 = urlsCount.takeOrdered(10)(new Ordering[Tuple2[String, Int]]() {  override def compare(x: (String, Int), y: (String, Int)): Int =      Ordering[Int].compare(y._2, x._2)})
    println("top 10")
    top10.foreach(println)
//    val urlsOrdered = urlsCount.sortBy()(new Ordering[Tuple2[String, Int]]() {
//  override def compare(x: (String, Int), y: (String, Int)): Int = 
//      Ordering[Int].compare(x._2, y._2)
//})
//    
//    println(maxKey2)
    
    sc.stop
    
  }

}