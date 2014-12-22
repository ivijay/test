package test

import org.apache.spark._

object StackOverFlowTest {
  
  def main(args: Array[String]) {
    
   val sc = new SparkContext(new SparkConf().setAppName("Gathering Data").setMaster("local"))

    val string1: String = "test string"
//	  	val csv = string1.map(_.mkString("|")).mkString("\n")mkString("\n")
//	  	println(sc.makeRDD[String](csv).getClass)
//	  	.saveAsSequenceFile("output99")
  }
}