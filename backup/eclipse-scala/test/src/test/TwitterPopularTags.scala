package test

import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
/**
 * Calculates popular hashtags (topics) over sliding 10 and 60 second windows from a Twitter
 * stream. The stream is instantiated with credentials and optionally filters supplied by the
 * command line arguments.
 *
 * Run this on your local machine as
 *
 */
object TwitterPopularTags {
	val WINDOW_LENGTH = new Duration(30 * 1000)
	val SLIDE_INTERVAL = new Duration(10 * 1000)
	System.setProperty("http.proxyHost", "172.20.181.136");
	System.setProperty("http.proxyPort", "8080");

	def main(args: Array[String]) {
		if (args.length < 4) {
			System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
					"<access token> <access token secret> [<filters>]")
					System.exit(1)
		}
		//		StreamingExamples.setStreamingLogLevels()
		val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
				val filters = args.takeRight(args.length - 4)
				// Set the system properties so that Twitter4j library used by twitter stream
				// can use them to generat OAuth credentials
		System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
		System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
		System.setProperty("twitter4j.oauth.accessToken", accessToken)
		System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
		val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local")
		val ssc = new StreamingContext(sparkConf, Seconds(2))
		val stream = TwitterUtils.createStream(ssc, None, filters)
		val tweets = stream.flatMap(status => {println("in tweets stream ");status.getText()}).window(WINDOW_LENGTH)

		println("before window stream RDD")
		tweets.foreachRDD(tweet => { println("in foreach rdd");tweet.foreach(println)
			if (tweet.count() > 0) {
				tweet.foreach(println)
			}
			else {
				println("No input recieved")
			}
		})

		ssc.start()
		ssc.awaitTermination(3)
	}
}