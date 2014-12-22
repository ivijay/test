package test
import org.apache.spark.Accumulable
import org.apache.spark.AccumulableParam
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object AccumulatorTest {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
		val sc = new SparkContext(conf)
		val a =Array(Array(1,2,3),Array(4,5,6))
		println("a value"+ a.deep.mkString("\n"))
		val rdd = sc.parallelize(a)
		val initialValue = Array.fill[Array[Int]](2)(Array.fill[Int](3)(1))
		println("initialValue  value"+ initialValue.deep.mkString("\n"))
		val accumulator = sc.accumulable(initialValue,"accumulatorvar")(MatrixAccumulatorParam)
		rdd.foreach{x=>
		accumulator += (x(0),0,0)
		accumulator += (x(1),0,1)
		accumulator += (x(2),0,2)
		accumulator += (x(0),1,0)
		accumulator += (x(1),1,1)
		accumulator += (x(2),1,2)
		println("accumulator local value in rdd is"+accumulator.localValue.deep.mkString("\n"))
		}

		println("after accumulation")
		println("accumulator value out of rdd is :" + accumulator.value.deep.mkString("\n") )
	}
}
object MatrixAccumulatorParam extends AccumulableParam[Array[Array[Int]], (Int, Int,   Int)] {

	def zero(initialValue: Array[Array[Int]]): Array[Array[Int]] = {
			initialValue
	}

	def addAccumulator(acc: Array[Array[Int]], value: (Int, Int, Int)): Array[Array[Int]] = {

	  println("in addAccumulator")
		acc(value._2)(value._3) = value._1
				acc

	}

	def addInPlace(m1: Array[Array[Int]], m2: Array[Array[Int]]): Array[Array[Int]] = {
	  println("in addInPlace")
	  println("m1 value:"+m1.deep.mkString("\n"))
	  println("m2 value:"+m2.deep.mkString("\n"))
		val columnLength: Int = m1.length
				val rowLength: Int = m1(0).length
				var updatedMatrix = Array.ofDim[Int](columnLength, rowLength)

				var i: Int = 0
				var j: Int = 0
				while (j < columnLength) {
				  i=0
					while (i < rowLength) {
					  println("m1(j)(i)"+ m1(j)(i))
					  println(" m2(j)(i))"+ m2(j)(i))
						val a = Math.max(m1(j)(i), m2(j)(i))
								updatedMatrix(j)(i) = a
								i += 1
					} 
					j += 1
				}

				updatedMatrix
	}


}