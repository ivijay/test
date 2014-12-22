package test

import org.apache.spark._

object test {
	def main(args:Array[String]) {
	
	println("Hello world")
	
//	val sc = new SparkContext("local","basic avg")
	val sc = new SparkContext(new SparkConf().setAppName("Gathering Data").setMaster("local"))
	
//	val input = List(100,2)
//	
//	var total = 0
//	var count =0
//	println(input.getClass());
//	input.map(x => {println(x);total +=x; count+=1})
//	var result  = input.fold(0)((x,y) => x+y)
//	
//	println("result = " + result/input.length)
	
	val inputFile = "/home/HadoopUser/Desktop/spark-all/examples/learning-spark/files/testweet.json"
    val sqlCtx = new org.apache.spark.sql.SQLContext(sc)
    val tweetTable = sqlCtx.jsonFile(inputFile)

    tweetTable.registerTempTable("tweetTable")
    
    tweetTable.printSchema
    
    sqlCtx.sql(
    "SELECT user.name FROM tweetTable LIMIT 10")
    .collect().foreach(println)
    
	}

}