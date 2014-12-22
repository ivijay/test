package test

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import java.io.File
import org.apache.spark.util.Utils
import org.apache.commons.io.FileUtils


object TweetAnalyzer {

	val WINDOW_LENGTH = new Duration(60 * 1000)
	val SLIDE_INTERVAL = new Duration(30 * 1000)

	def main(arg:Array[String]) {


		println("start of program")
		val sparkConf = new SparkConf().setMaster("local").setAppName("Tweet Analyzer").set("spark.files.overwrite","0")
		val sc= new SparkContext(sparkConf);
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

//		println("before window dstream");

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

//				val tweetsSenti = results.map(tweetText=>  {val tweetWordsSentiment = tweetText.toString.split(" ").map(word => {var senti :Int= 0;if(dictionary.filter(x=> x._1 == word).map(y=>y._2).collect().length > 0) {senti = dictionary.filter(x=> x._1 == word.toLowerCase()).map(y=>y._2).collect()(0)};senti});
				val tweetsSenti = results.map(tweetText=>  {val tweetWordsSentiment = tweetText.toString.split(" ").map(word => {var senti :Int= 0;if(dictionary.lookup(word.toLowerCase()).length > 0) {senti = dictionary.lookup(word.toLowerCase())(0)};senti});
				val tweetSentiment = tweetWordsSentiment.sum;
				 (tweetSentiment,tweetText.toString)})

				 val tweetsSentiRDD : org.apache.spark.rdd.RDD [(Int,String)]= sc.parallelize(tweetsSenti.toList).sortBy(x=> x._1, false )
				 
//								println("tweet: " + tweetText);
//				println("tweetSentiment: " + tweetSentiment);

				val hashTags = results.flatMap(tweetText => tweetText.toString.split(" ").filter(_.startsWith("#")))

				val hashTagsRDD : org.apache.spark.rdd.RDD[String] = sc.parallelize(hashTags.toList)
				
				
				val topCounts  = hashTagsRDD.map(x=>(x.toString().replaceAll("[^a-zA-Z0-9]", ""), 1)).reduceByKey(_+_)
//				val top5 =  topCounts.takeOrdered(5)(new Ordering[Tuple2[String, Int]]() {  override def compare(x: (String, Int), y: (String, Int)): Int =      Ordering[Int].compare(y._2, x._2)})
				val top5 =  topCounts.map(x=> (x._2,x._1)).sortByKey(false).take(5)
				val least5 =  topCounts.map(x=> (x._2,x._1)).sortByKey(true).take(5)
//				println(top5.getClass())
				
				
				val retweets = sqlContext.sql("select retweet_count,text from IncomingtweetsTable ORDER BY retweet_count DESC LIMIT 10").collect;
				
				val retweetsOrdered =  retweets.map(row => { if (row.length >=2 ) { (row.getInt(0),row.getString(1))}})
				
				//
				
				
				val tweetsFile = new File("/home/HadoopUser/Desktop/tweet analyzer/tweets-collection.txt");
				if(tweetsFile.exists()) {
				FileUtils.deleteDirectory(tweetsFile);
				}
				
				val top5File = new File("/home/HadoopUser/Desktop/tweet analyzer/hashtags-top5.txt");
				if(top5File.exists()) {
				FileUtils.deleteDirectory(top5File);
				}
				
				val least5File = new File("/home/HadoopUser/Desktop/tweet analyzer/hashtags-least5.txt");
				if(least5File.exists()) {
				FileUtils.deleteDirectory(least5File);
				}
				
				val retweetsFile = new File("/home/HadoopUser/Desktop/tweet analyzer/retweets.txt");
				if(retweetsFile.exists()) {
				FileUtils.deleteDirectory(retweetsFile);
				}
				
				println("All hashtags occurance count")
				topCounts.foreach(println)
				println("Top 5 hashtags")
				top5.foreach(println)
				println("Least 5 hashtags")
				least5.foreach(println)
				println("All tweets with sentiment")
				tweetsSentiRDD.foreach(println)
				println("popular retweets")
				retweets.foreach(println)

				//write all files
				
				
				tweetsSentiRDD.saveAsTextFile("/home/HadoopUser/Desktop/tweet analyzer/tweets-collection.txt");
				sc.parallelize(top5.toList).saveAsTextFile("/home/HadoopUser/Desktop/tweet analyzer/hashtags-top5.txt");
				sc.parallelize(least5.toList).saveAsTextFile("/home/HadoopUser/Desktop/tweet analyzer/hashtags-least5.txt");
				sc.parallelize(retweetsOrdered.toList).saveAsTextFile("/home/HadoopUser/Desktop/tweet analyzer/retweets.txt");
				
			}
		});
		//    tweetTable.registerTempTable("tweetTable");
		//    tweetTab le.printSchema

		streamingContext.start();
		streamingContext.awaitTermination();
	}



}