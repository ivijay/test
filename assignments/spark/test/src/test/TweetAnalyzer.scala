package test

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}

object TweetAnalyzer {

	val WINDOW_LENGTH = new Duration(60 * 1000)
	val SLIDE_INTERVAL = new Duration(30 * 1000)

	def main(arg:Array[String]) {


		println("start of program")
		val sc= new SparkContext("local","tweet analyzer");
		val sqlContext = new SQLContext(sc);
		val streamingContext = new StreamingContext(sc,SLIDE_INTERVAL);

		Logger.getRootLogger.setLevel(Level.ERROR)
		val tweetsDStream = streamingContext.textFileStream("/home/HadoopUser/Downloads/target");

		//    val tweetTable = sqlContext.jsonFile("/home/HadoopUser/Downloads/target");

		val tweetsDStreamFilter = tweetsDStream.map(x=>x).cache();  // all the filter conditions on incoming tweets goes here

//		println("tweetsDStreamFilter count: " + tweetsDStreamFilter.count);
		val windowDStream = tweetsDStreamFilter.window(WINDOW_LENGTH,SLIDE_INTERVAL);

		val inputfile = sc.textFile("/home/HadoopUser/Downloads/AFINN-111.txt");
		val dictionary = inputfile.map(x=>(x.split("\t")(0),x.split("\t")(1).toInt));
		//		val value1:Int = dictionary.filter(x=> x._1 == "abandon").map(y=>y._2).collect()(0)

		println("before window dstream");

		windowDStream.foreachRDD(Incomingtweets => {
			println("in windowdstream")
			if (Incomingtweets.count() == 0) {
				println("No tweet received in this time interval")
			} else {
				println("processing tweets received in this interval")
				val IncomingtweetsTable = sqlContext.jsonRDD(Incomingtweets);
				IncomingtweetsTable.registerAsTable("IncomingtweetsTable");
//				IncomingtweetsTable.printSchema;

				val results = sqlContext.sql("select text from IncomingtweetsTable").collect;

				val tweetsSenti = results.map(tweetText=>  {val tweetWordsSentiment = tweetText.toString.split(" ").map(word => {var senti :Int= 0;if(dictionary.filter(x=> x._1 == word).map(y=>y._2).collect().length > 0) {senti = dictionary.filter(x=> x._1 == word.toLowerCase()).map(y=>y._2).collect()(0)};senti});
				val tweetSentiment = tweetWordsSentiment.sum;
				 (tweetSentiment,tweetText.toString)})

				 
				 val tweetsSentiRDD : org.apache.spark.rdd.RDD [(Int,String)]= sc.parallelize(tweetsSenti.toList).sortBy(x=> x._1, false )
				 
				 tweetsSentiRDD.foreach(println)
//								println("tweet: " + tweetText);
//				println("tweetSentiment: " + tweetSentiment);

				val hashTags = results.flatMap(tweetText => tweetText.toString.split(" ").filter(_.startsWith("#")))

				val hashTagsRDD : org.apache.spark.rdd.RDD[String] = sc.parallelize(hashTags.toList)
				
				
				val topCounts  = hashTagsRDD.map(x=>(x.toString().replaceAll("""([{}]|\\["n])""", ""), 1)).reduceByKey(_+_)
				val top5 =  topCounts.takeOrdered(5)(new Ordering[Tuple2[String, Int]]() {  override def compare(x: (String, Int), y: (String, Int)): Int =      Ordering[Int].compare(y._2, x._2)})
				println(top5.getClass())
				top5.foreach(println)
				
			}
		});
		//    tweetTable.registerTempTable("tweetTable");
		//    tweetTab le.printSchema

		streamingContext.start();
		streamingContext.awaitTermination();
	}



}