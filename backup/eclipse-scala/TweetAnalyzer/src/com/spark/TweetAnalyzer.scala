package com.spark

import java.io._

import twitter4j._;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.sql._
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.util.Utils
import org.apache.commons.io.FileUtils

/**
 *
 * TweetAnalyzer.scala
 * Purpose: This is a simple spark-scala application for realtime sentiment analysis on twitter tweets.
 * 
 * Following trends are being computed in this application 
 * Top 5 hashtags
 * Least 5 hashtags
 * Popular Retweets
 * Sentiment of each collected tweet
 *
 * @author Vijay
 * @version 1.0
 * @param ConsumerKey ConsumerSecret AccessToken AccessTokenSecret trackingKeyword[s]
 *
 */
object TweetAnalyzer {

	val WINDOW_LENGTH = new Duration(60 * 1000)
	val SLIDE_INTERVAL = new Duration(30 * 1000)
	val dir: String = "Statuses"
	/**
	 * Proxy settings
	 */
	System.setProperty("http.proxyHost", "172.20.181.136");
	System.setProperty("http.proxyPort", "8080");

	/**
	 * method: collect starts a twitter4j listener and captures all the tweets generated with the given keywords.
	 */
	def collect(args: Array[String]) {
		val cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true);
		cb.setJSONStoreEnabled(true);
		cb.setOAuthConsumerKey(args(0));
		cb.setOAuthConsumerSecret(args(1));
		cb.setOAuthAccessToken(args(2));
		cb.setOAuthAccessTokenSecret(args(3));
		
		System.setProperty("http.proxyHost", "172.20.181.136");
		System.setProperty("http.proxyPort", "8080");

		println("started twitter collect ")

		val f = new File(dir)
		if (f.exists()) {
			FileUtils.deleteDirectory(f);
		}
		f.mkdir();

		val listener = new StatusListener() {
			/**
			 * method: onStatus defines the action when a tweet for the keyword is received
			 */
			@Override
			def onStatus(status: Status) {
				val rawstatus: String = DataObjectFactory.getRawJSON(status);
				val fileName: String = status.getId() + ".json";
				storeJSON(rawstatus, dir, fileName);
			}
			@Override
			def onTrackLimitationNotice(arg0: Int) {}
			@Override
			def onStallWarning(arg0: StallWarning) {}
			@Override
			def onException(arg0: Exception) { arg0.printStackTrace() }
			@Override
			def onDeletionNotice(arg0: StatusDeletionNotice) {}
			@Override
			def onScrubGeo(arg0: Long, arg1: Long) {}
		};

		val keywords = args.takeRight(args.length - 4)
				//		System.out.print("monitoring for the keywords: ");
				//		for(s : String <- keywords) { 
				//			System.out.print(s + "\t");
				//		}

				val fq = new FilterQuery();
		fq.track(keywords);

		val twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		twitterStream.addListener(listener);
		twitterStream.filter(fq);

	}
	/**
	 * method: storeJSON stores the incoming tweets in JSON format on local file system
	 * NOTE: This can be configured to HDFS if executed in cluster environment.
	 */
	def storeJSON(rawJSON: String, dir: String, fileName: String) {

//		println("fileName:" + fileName)
//		println("rawJSON: " + rawJSON)

		val f2 = new File(new String(dir + "/" + fileName))
//		println(f2.getAbsolutePath())
		scala.tools.nsc.io.File(f2).writeAll(rawJSON)

	}

	/**
	 * process each tweet received
	 */
	def main(args: Array[String]) {

		println("start of program")
		/**
		 * Validation for parameters i.e, Four twitter application keys and tracking keywords (minimum one keyword)
		 */
		if (args.length < 5) {
			System.err.println("Required paramaters are missing");
			System.exit(0);
		}
		
		Logger.getRootLogger.setLevel(Level.ERROR);
		
		collect(args);

		/**
		 * Instantiate the various spark context variables
		 */
		val sparkConf = new SparkConf().setAppName("Tweet Analyzer").setMaster("local");
		val sc = new SparkContext(sparkConf);
		val sqlContext = new SQLContext(sc);
		val streamingContext = new StreamingContext(sc, SLIDE_INTERVAL);


		/**
		 * monitors the directory dir for any new files.
		 */
		val tweetsDStream = streamingContext.textFileStream(dir);

		val tweetsDStreamFilter = tweetsDStream.map(x => x).cache();

		/**
		 * Set the total window length for processing the received tweets.
		 * Here it is configured for 60 Seconds.
		 * Also sets the Sliding interval.
		 * Here it is 30 Seconds.
		 */
		val windowDStream = tweetsDStreamFilter.window(WINDOW_LENGTH, SLIDE_INTERVAL);

		/**
		 * AFINN words list are loaded into key value dictionary for lookup
		 *
		 *  	More details regarding AFINN can be found at below links
		 * http://arxiv.org/abs/1103.2903
		 * http://stackoverflow.com/questions/18246964/where-can-i-find-sentiment-based-categorical-dictionary
		 */
		
		val currentDir = System.getProperty("user.dir");
		
		val inputfile = sc.textFile("AFINN-111.txt");

		val dictionary = inputfile.map(x => (x.split("\t")(0), x.split("\t")(1).toInt));
		
		/**
		 * Code to process the incoming tweets received in the WINDOW_LENGTH duration
		 */
		windowDStream.foreachRDD(Incomingtweets => {
			if (Incomingtweets.count() == 0) {
				println("No tweet received in this time interval");
			} else {
				println("processing tweets received in this interval");
				/**
				 * Store the received tweets in temporary table
				 */
				val IncomingtweetsTable = sqlContext.jsonRDD(Incomingtweets);
				IncomingtweetsTable.registerAsTable("IncomingtweetsTable");
				//				IncomingtweetsTable.printSchema;

				/**
				 * select tweet text from each tweet and return as RDD Array
				 */
				val results = sqlContext.sql("select text from IncomingtweetsTable").collect;

				val tweetsSenti = results.map(tweetText => {
					val tweetWordsSentiment = tweetText.toString.split(" ").map(word => { var senti: Int = 0; if (dictionary.lookup(word.toLowerCase()).length > 0) { senti = dictionary.lookup(word.toLowerCase())(0) }; senti });
					val tweetSentiment = tweetWordsSentiment.sum;
					(tweetSentiment, tweetText.toString)
				})

				val tweetsSentiRDD: org.apache.spark.rdd.RDD[(Int, String)] = sc.parallelize(tweetsSenti.toList).sortBy(x => x._1, false);

				val hashTags = results.flatMap(tweetText => tweetText.toString.split(" ").filter(_.startsWith("#")));

				val hashTagsRDD: org.apache.spark.rdd.RDD[String] = sc.parallelize(hashTags.toList);

				val topCounts = hashTagsRDD.map(x => (x.toString().replaceAll("[^a-zA-Z0-9]", ""), 1)).reduceByKey(_ + _);
				val top5 = topCounts.map(x => (x._2, x._1)).sortByKey(false).take(5);
				val least5 = topCounts.map(x => (x._2, x._1)).sortByKey(true).take(5);

				val retweets = sqlContext.sql("select retweet_count,text from IncomingtweetsTable ORDER BY retweet_count DESC LIMIT 10").collect;

				val retweetsOrdered = retweets.map(row => { if (row.length >= 2) { (row.getInt(0), row.getString(1)) } });

				/**
				 * Pre-delete the existing output directories
				 */

				val tweetsFile = new File("tweets-collection.txt");
				if (tweetsFile.exists()) {
					FileUtils.deleteDirectory(tweetsFile);
				}

				val top5File = new File("hashtags-top5.txt");
				if (top5File.exists()) {
					FileUtils.deleteDirectory(top5File);
				}

				val least5File = new File("hashtags-least5.txt");
				if (least5File.exists()) {
					FileUtils.deleteDirectory(least5File);
				}

				val retweetsFile = new File("retweets.txt");
				if (retweetsFile.exists()) {
					FileUtils.deleteDirectory(retweetsFile);
				}

				/*
				 * Debug print statements
				 */
				println("All hashtags occurance count");
				topCounts.foreach(println);
				println("Top 5 hashtags");
				top5.foreach(println);
				println("Least 5 hashtags");
				least5.foreach(println);
				println("All tweets with sentiment");
				tweetsSentiRDD.foreach(println);
				println("popular retweets");
				retweets.foreach(println);

				

				//				println(currentDir);

				//Save the results

				tweetsSentiRDD.saveAsTextFile(currentDir + "/tweets-collection.txt");
				sc.parallelize(top5.toList).saveAsTextFile(currentDir + "/hashtags-top5.txt");
				sc.parallelize(least5.toList).saveAsTextFile(currentDir + "/hashtags-least5.txt");
				sc.parallelize(retweetsOrdered.toList).saveAsTextFile(currentDir + "/retweets.txt");
			}
		});
		/*
		 * Start the streaming
		 */
		streamingContext.start();
		streamingContext.awaitTermination();
	}
}
