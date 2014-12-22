package test


import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql._
import org.apache.spark.sql

object Temp {
case class Score(name: String, score: Int)
case class Age(name: String, age: Int)

def main(args: Array[String]) {

	val sparkConf = new SparkConf().setMaster("local").setAppName("SPARK SQL JOIN")
			val sc= new SparkContext(sparkConf)
	val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	import sqlContext._
	val scores = sc.textFile("scores.txt").map(_.split(",")).map(s => Score(s(0),s(1).trim.toInt))
	val ages = sc.textFile("ages.txt").map(_.split(",")).map(s => Age(s(0),s(1).trim.toInt))
	scores.registerAsTable("scores")
	ages.registerAsTable("ages")
	val joined = sqlContext.sql("""
			SELECT a.name, a.age, s.score
			FROM ages a JOIN scores s
			ON a.name = s.name""")
			joined.collect().foreach(println)

	val scoresAliased = scores.as('s)
	val agesAliased = ages.as('a)
//	val joined = scoresAliased.join(agesAliased, Inner, Some("s.name".attr === "a.name".attr))


}
}