package test

import scala.collection.mutable
import org.apache.spark.SparkContext

object ScalaMapTest {
  
  def main(args: Array[String]) {
    
    println("test")
    
    val sc = new SparkContext("local","dictionary app");
    val inputfile = sc.textFile("/home/HadoopUser/Downloads/AFINN-111.txt")
    val dictionary = inputfile.map(x=>(x.split("\t")(0),x.split("\t")(1).toInt));
    val arraytest=dictionary.collect();
    val mapres=arraytest.toMap
    
//    val value:Int=mapres.get("abandon").asInstanceOf[Int]
    val query:String = "asdjfasjdfljlasdfkj"
    val value1:Int = dictionary.filter(x=> x._1 == query).map(y=>y._2).collect()(0)
    println(value1)
    
//    value match{case Some(i)=>println(i);case i=>println(i)}
  
  }

}