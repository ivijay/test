
import java.io._

import twitter4j._;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.util.Utils
import org.apache.commons.io.FileUtils

object TweetAnalyzer {

	val WINDOW_LENGTH = new Duration(60 * 1000)
	val SLIDE_INTERVAL = new Duration(30 * 1000)
	val dir: String = "Statuses"
	System.setProperty("http.proxyHost", "172.20.181.136");
	System.setProperty("http.proxyPort", "8080");

	def collect(args:Array[String]) {
		val cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true);
		cb.setJSONStoreEnabled(true);
		cb.setOAuthConsumerKey(args(0));
		cb.setOAuthConsumerSecret(args(1));
		cb.setOAuthAccessToken(args(2));
		cb.setOAuthAccessTokenSecret(args(3));

		val f = new File(dir)
		if(f.exists()) {		
			FileUtils.deleteDirectory(f);
		}
		f.mkdir();

		val listener = new StatusListener() {
			@Override
			def onStatus(status: Status) {
				val rawstatus: String = DataObjectFactory.getRawJSON(status);
			val fileName: String = status.getId() + ".json";
			storeJSON(rawstatus, dir, fileName);
			}
			@Override
			def onTrackLimitationNotice(arg0: Int ) { }
			@Override
			def onStallWarning(arg0: StallWarning) { }
			@Override
			def onException(arg0: Exception) { arg0.printStackTrace() }
			@Override
			def onDeletionNotice(arg0: StatusDeletionNotice) { }
			@Override
			def onScrubGeo(arg0: Long, arg1:Long ) { }
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
	def storeJSON(rawJSON:String , dir:String,fileName:String ) {

		println("fileName:" + fileName)
		println("rawJSON: " + rawJSON)

		val f2 = new File(new String(dir+"/"+fileName))
		println(f2.getAbsolutePath())
		scala.tools.nsc.io.File(f2).writeAll(rawJSON)

	}

	def main(args:Array[String]) {


		println("start of program")
		if (args.length < 5) {
			System.err.println("Required paramaters are missing");
			System.exit(0);
		}

		collect(args);

//		val sparkConf = new SparkConf().setMaster("local").setAppName("Tweet Analyzer").set("spark.files.overwrite","0");
		val sparkConf = new SparkConf().setAppName("Tweet Analyzer").set("spark.files.overwrite","0");
		val sc= new SparkContext(sparkConf);
		val sqlContext = new SQLContext(sc);
		val streamingContext = new StreamingContext(sc,SLIDE_INTERVAL);

		Logger.getRootLogger.setLevel(Level.ERROR);

		val tweetsDStream = streamingContext.textFileStream(dir);

		val tweetsDStreamFilter = tweetsDStream.map(x=>x).cache();  

		val windowDStream = tweetsDStreamFilter.window(WINDOW_LENGTH,SLIDE_INTERVAL);

		val inputfile = sc.textFile("/home/HadoopUser/Downloads/AFINN-111.txt");

		val dictionary = inputfile.map(x=>(x.split("\t")(0),x.split("\t")(1).toInt));

		windowDStream.foreachRDD(Incomingtweets => {
			if (Incomingtweets.count() == 0) {
				println("No tweet received in this time interval");
			} else {
				println("processing tweets received in this interval");
				val IncomingtweetsTable = sqlContext.jsonRDD(Incomingtweets);
				IncomingtweetsTable.registerAsTable("IncomingtweetsTable");
				//				IncomingtweetsTable.printSchema;

				val results = sqlContext.sql("select text from IncomingtweetsTable").collect;

				val tweetsSenti = results.map(tweetText=>  {val tweetWordsSentiment = tweetText.toString.split(" ").map(word => {var senti :Int= 0;if(dictionary.lookup(word.toLowerCase()).length > 0) {senti = dictionary.lookup(word.toLowerCase())(0)};senti});
				val tweetSentiment = tweetWordsSentiment.sum;
				(tweetSentiment,tweetText.toString)})

				val tweetsSentiRDD : org.apache.spark.rdd.RDD [(Int,String)]= sc.parallelize(tweetsSenti.toList).sortBy(x=> x._1, false );

				val hashTags = results.flatMap(tweetText => tweetText.toString.split(" ").filter(_.startsWith("#")));

				val hashTagsRDD : org.apache.spark.rdd.RDD[String] = sc.parallelize(hashTags.toList);

				val topCounts  = hashTagsRDD.map(x=>(x.toString().replaceAll("[^a-zA-Z0-9]", ""), 1)).reduceByKey(_+_);
				val top5 =  topCounts.map(x=> (x._2,x._1)).sortByKey(false).take(5);
				val least5 =  topCounts.map(x=> (x._2,x._1)).sortByKey(true).take(5);

				val retweets = sqlContext.sql("select retweet_count,text from IncomingtweetsTable ORDER BY retweet_count DESC LIMIT 10").collect;

				val retweetsOrdered =  retweets.map(row => { if (row.length >=2 ) { (row.getInt(0),row.getString(1))}});



				val tweetsFile = new File("tweets-collection.txt");
				if(tweetsFile.exists()) {
					FileUtils.deleteDirectory(tweetsFile);
				}

				val top5File = new File("hashtags-top5.txt");
				if(top5File.exists()) {
					FileUtils.deleteDirectory(top5File);
				}

				val least5File = new File("hashtags-least5.txt");
				if(least5File.exists()) {
					FileUtils.deleteDirectory(least5File);
				}

				val retweetsFile = new File("retweets.txt");
				if(retweetsFile.exists()) {
					FileUtils.deleteDirectory(retweetsFile);
				}

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

				//write all files

				val currentDir = System.getProperty("user.dir");

				println(currentDir);

				tweetsSentiRDD.saveAsTextFile(currentDir+"/tweets-collection.txt");
				sc.parallelize(top5.toList).saveAsTextFile(currentDir+"/hashtags-top5.txt");
				sc.parallelize(least5.toList).saveAsTextFile(currentDir+"/hashtags-least5.txt");
				sc.parallelize(retweetsOrdered.toList).saveAsTextFile(currentDir+"/retweets.txt");
			}
		});
		streamingContext.start();
		streamingContext.awaitTermination();
	}
}