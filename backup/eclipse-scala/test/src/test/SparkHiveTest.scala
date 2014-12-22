package test

import org.apache.spark.{SparkContext,SparkConf};
import org.apache.spark.sql.SQLContext

object SparkHiveTest {

  def main(args: Array[String]) {
    
    val sc = new SparkContext(new SparkConf().setAppName("spark hive test").setMaster("local"));
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc);
    sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    sqlContext.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")
    sqlContext.sql("FROM src SELECT key, value").collect().foreach(println)
  }
}