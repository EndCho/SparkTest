package cn.rzlee.spark.scala

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BroadcastVariable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val factor = 3
    val factorBroadcast: Broadcast[Int] = sc.broadcast(factor)
    
    val numbersArray = Array(1,2,3,4,5,6,7,8,9)
    val numbers: RDD[Int] = sc.parallelize(numbersArray, 1)
    val mutipleNumbers: RDD[Int] = numbers.map(num =>num * factorBroadcast.value)

    mutipleNumbers.foreach(num=>println(num))
    sc.stop()
  }
}
