package test

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftOuter, RightOuter}

object SparkSQLJoin {

case class Item(id:String, name:String, unit:Int, companyId:String)
case class Company(companyId:String, name:String, city:String)

def main(args: Array[String]) {

  	val sparkConf = new SparkConf().setMaster("local").setAppName("SPARK SQL JOIN")
	val sc= new SparkContext(sparkConf)
	val sqlContext = new SQLContext(sc)
  	
  	import sqlContext.createSchemaRDD

	val i1 = Item("1", "first", 1, "c1")
	val i2 = Item("2", "second", 2, "c2")
	val i3 = Item("3", "thied", 3, "c3")
	val c1 = Company("c1", "company-1", "city-1")
	val c2 = Company("c2", "company-2", "city-2")

	val companies = sc.parallelize(List(c1,c2))
	companies.registerAsTable("companies")

	val items = sc.parallelize(List(i1,i2,i3))
	items.registerAsTable("items")

	val result = sqlContext.sql("SELECT * FROM companies C JOIN items I ON C.companyId= I.companyId").collect
	
	result.foreach(println)

}
}