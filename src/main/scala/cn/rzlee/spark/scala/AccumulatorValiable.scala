package cn.rzlee.spark.scala

import org.apache.spark.{Accumulable, Accumulator, SparkConf, SparkContext}

object AccumulatorValiable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(conf)

    val sum: Accumulator[Int]  = sc.accumulator(0)
    val numbersArray = Array(1,2,3,4,5,6,7,8,9)

    val numbers = sc.parallelize(numbersArray,1)
    numbers.foreach(number=>sum+=number)
    println(sum.value)
  }
}
