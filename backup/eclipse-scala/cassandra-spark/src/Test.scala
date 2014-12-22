
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.datastax.spark.connector._

object Test {
def main(args: Array[String]) {
val conf = new SparkConf(true).set("spark.cassandra.connection.host", "172.20.95.120")
val sc = new SparkContext("spark://01hw508224.India.TCS.COM:7077", "test", conf)
val rdd = sc.cassandraTable("erm", "users")
	rdd.toArray.foreach(println)
	}
}

