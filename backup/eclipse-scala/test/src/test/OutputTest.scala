package test

import org.apache.spark._

object OutputTest {

	def main(args:Array[String]) {
		val sc = new SparkContext("local","output test")

		val inputFile = "/home/HadoopUser/Desktop/spark-all/examples/learning-spark/files/testweet.json"

		val words = sc.textFile(inputFile, 2)
		
		val pairRDDtest = words.flatMap(x=>x.split(" ")).map(x=>(x,x.length))

		println(pairRDDtest.getClass())
		
		pairRDDtest.foreach(println)
		pairRDDtest.saveAsTextFile("output1")
		
		





	}
}