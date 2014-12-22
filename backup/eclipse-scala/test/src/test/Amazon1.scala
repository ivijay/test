package test

import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql._
import scala.collection.mutable.{Map,SynchronizedMap, HashMap}
import org.apache.spark.rdd.{EmptyRDD, HadoopRDD, RDD, UnionRDD, NewHadoopRDD}
import org.apache.hadoop.io._;
import wordcount._;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.sql._;
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftOuter, RightOuter}



object Amazon1 {
// 	case class AmazonItem(Id:Int, ASIN:String, title:String, group:String, salesrank:String,similarCount:Int,similarCountASIN: Array[Byte],categoriesCount:Int,categoriesText: Array[Byte],reviews_total:Int,reviews_downloaded:Int,reviews_avgrating:Int, reviews:Array[Byte])
  	case class AmazonItem1(Id:Int, ASIN:String, title:String, group:String, salesrank:String)
  	case class AmazonItemSimilar(Id:Int, similarASIN:String)
  	case class AmazonItemCategories(Id:Int, similarASIN:String)
  	case class AmazonItemReviews(Id:Int, similarASIN:String)

	def main(args:Array[String]){

  println("program started");
  
  val sparkConf = new SparkConf().setMaster("local").setAppName("amazon data test")
  val sc = new SparkContext(sparkConf );
  val sqlContext = new SQLContext(sc)
  	
  import sqlContext._;

  val inFilePath = "/home/HadoopUser/Downloads/test-input.txt";
  val amazondata = sc.textFile(inFilePath,1);
//  hadoopConfiguration
//  val hadoopRDD = sc.newAPIHadoopFile(inFilePath, classOf[ParagraphInputFormat], classOf[LongWritable], classOf[Text])
//.asInstanceOf[NewHadoopRDD[_, _]]
//.mapPartitionsWithInputSplit { (split, part) =>
//Iterator(split.asInstanceOf[NewFileSplit].getPath.toUri.getPath)
//}.collect()
  
  val seperator = "\t"

  val file = sc.newAPIHadoopFile[LongWritable, Text, ParagraphInputFormat](inFilePath)
  
//  file.foreach(println)
  val itemsRDD = file.map(x=>{
//    println("in map")
    val item_lines = x._2.toString().split("\n");
    
    var id_value:Int=0;
    
    val id= item_lines(0).split(":")(0).trim()
    
    if (id.toString()=="Id") {
      try{
      id_value=item_lines(0).split(":")(1).trim().toInt
//      println(id_value)
      } catch {
        case e:Exception => {
          id_value=0;
        }
      }
    }
    
//    if(id_value>0) {
      
      val ASIN_value= if(item_lines(1).split(":")(0).trim().equals("ASIN")) item_lines(1).split(":",2)(1).trim(); 
      val title_value = if(item_lines(2).split(":")(0).trim().equals("title")) item_lines(2).split(":",2)(1).trim();
      val group_value = if(item_lines(3).split(":")(0).trim().equals("group")) item_lines(3).split(":",2)(1).trim();
      val salesrank_value = if(item_lines(4).split(":")(0).trim().equals("salesrank")) item_lines(4).split(":",2)(1).trim();
      val similar_value = if(item_lines(5).split(":")(0).trim().equals("similar")) item_lines(5).split(":",2)(1).trim().toString();
      		val similar_value_count = similar_value.toString.split("  ")(0).trim().toInt
      		val similar_value_text: Array[String] = similar_value.toString.split("  ").map(x=>x.trim())
      		val similar_value_text_filtered: Array[String] = Array.fill(similar_value_count)("") 
      		  for(i <- 1 to similar_value_count){ similar_value_text_filtered(i-1)=similar_value_text(i)}  
      val categories_count_value = if(item_lines(6).split(":")(0).trim().equals("categories")) item_lines(6).split(":",2)(1).trim().toString().toInt;
      val categories_text_value: Array[String] = Array.fill(categories_count_value.toString.toInt){""};
      
      var index =7;
      for(i <- 0 to (categories_count_value.toString.toInt-1)) {
        categories_text_value(i) =  item_lines(index).trim()
        index+=1;
      }
      val reviews_value = if(item_lines(index).split(":")(0).trim().equals("reviews")) item_lines(index).split(":",2)(1).trim().toString();
      val reviews_count_value = if(reviews_value.toString.split(":")(0).trim().equals("total")) reviews_value.toString.split(":")(1).trim().split(" ")(0)
      val reviews_value_split = reviews_value.toString.split("  ")
      val reviews_downloaded_value = if(reviews_value_split(1).trim().toString().split(":")(0).trim().toString() == "downloaded") reviews_value_split(1).trim().toString().split(":")(1).trim().toString().toInt  
      val reviews_rating_value = if(reviews_value_split(2).trim().toString().split(":")(0).trim().toString() == "avg rating") reviews_value_split(2).trim().toString().split(":")(1).trim().toString().toFloat
      index+=1;
      val reviews_text_value: Array[String] = Array.fill(reviews_count_value.toString.toInt){""};
      for(i <- 0 to (reviews_count_value.toString.toInt-1)) {
        reviews_text_value(i) =  item_lines(index).trim()
        index+=1;
      }
      
      
//    }
      
    val result = id_value + seperator + ASIN_value.toString + seperator + title_value.toString + seperator + group_value.toString + seperator + salesrank_value.toString + seperator + similar_value_count + seperator + similar_value_text_filtered.toList+ seperator + 
categories_count_value.toString.toInt + seperator + categories_text_value.toList + seperator + reviews_count_value.toString.toInt + seperator + reviews_downloaded_value  + seperator + reviews_rating_value + seperator +  reviews_text_value.toList;
//id_value, ASIN_value.toString, title_value.toString, group_value.toString, salesrank_value.toString,similar_value_count,similar_value_text.toString().getBytes(),
//categories_count_value.toString.toInt,categories_text_value.toString().getBytes(),reviews_count_value.toString.toInt,0,0, reviews_text_value.toString().getBytes())
//      AmazonItem1(id_value, ASIN_value.toString, title_value.toString, group_value.toString, salesrank_value.toString)
      

    result

    });
  
    
    ////////////////////////////
//    itemsRDD.registerAsTable("AmazonItems")
//    val result = sqlContext.sql("SELECT * FROM AmazonItems").collect
//    itemsRDD.foreach(println)
      val AmazonItem1RDD = itemsRDD.map(x=> {val x1 = x.split(seperator); AmazonItem1(x1(0).trim().toInt,x1(1),x1(2),x1(3),x1(4))});
      AmazonItem1RDD.registerAsTable("AmazonItem1RDD")
      sqlContext.sql("SELECT * FROM AmazonItem1RDD").collect.foreach(println)
      
      val AmazonItemSimilarRDDtemp1 = itemsRDD.map(x=> {val x1 = x.split(seperator);(x1(0).trim().toInt,x1(6))});
      val AmazonItemSimilarRDDtemp2 = AmazonItemSimilarRDDtemp1.flatMap(x=> {
          val tempList = x._2.split(", ").map(x=> {val temp2 = x.replace("List(", "").replace(")","");temp2})
          val temp = for { fromList <- tempList.toList } yield (x._1, fromList);
          temp
        });
        val AmazonItemSimilarRDD = AmazonItemSimilarRDDtemp2.map(x=> AmazonItemSimilar(x._1,x._2));
        AmazonItemSimilarRDD.registerAsTable("AmazonItemSimilarRDD");
        sqlContext.sql("SELECT * FROM AmazonItemSimilarRDD").collect.foreach(println);

      val AmazonItemCategoriesRDDtemp1 = itemsRDD.map(x=> {val x1 = x.split(seperator);(x1(0).trim().toInt,x1(8))});
      val AmazonItemCategoriesRDDtemp2 = AmazonItemCategoriesRDDtemp1.flatMap(x=> {
          val tempList = x._2.split(", ").map(x=> {val temp2 = x.replace("List(", "").replace(")","");temp2})
          val temp = for { fromList <- tempList.toList } yield (x._1, fromList);
          temp
        });
        val AmazonItemCategoriesRDD = AmazonItemCategoriesRDDtemp2.map(x=> AmazonItemCategories(x._1,x._2));
        AmazonItemCategoriesRDD.registerAsTable("AmazonItemCategoriesRDD");
        sqlContext.sql("SELECT * FROM AmazonItemCategoriesRDD").collect.foreach(println);


      val AmazonItemReviewsRDDtemp1 = itemsRDD.map(x=> {val x1 = x.split(seperator);(x1(0).trim().toInt,x1(12))});
      val AmazonItemReviewsRDDtemp2 = AmazonItemReviewsRDDtemp1.flatMap(x=> {
          val tempList = x._2.split(", ").map(x=> {val temp2 = x.replace("List(", "").replace(")","");temp2})
          val temp = for { fromList <- tempList.toList } yield (x._1, fromList);
          temp
        });
        val AmazonItemReviewsRDD = AmazonItemReviewsRDDtemp2.map(x=> AmazonItemReviews(x._1,x._2));
        AmazonItemReviewsRDD.registerAsTable("AmazonItemReviewsRDD");
        sqlContext.sql("SELECT * FROM AmazonItemReviewsRDD").collect.foreach(println);

        
//      println("AmazonItemSimilarRDD"+AmazonItemSimilarRDD.getClass());
//      AmazonItemSimilarRDD.foreach(x => println(x._1,x._2))
      
//  	case class AmazonItemSimilar(Id:Int, similarASIN:String)
//  	case class AmazonItemCategories(Id:Int, similarASIN:String)
//  	case class AmazonItemReviews(Id:Int, similarASIN:String)
    
  }
//  amazondata
  
  
  }

