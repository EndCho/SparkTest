package cn.rzlee.spark.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LineCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[1]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("C:\\Users\\txdyl\\Desktop\\log\\in\\data.txt",1)
    val pairs: RDD[(String, Int)] = lines.map(line=>(line,1))
    val lineCount: RDD[(String, Int)] = pairs.reduceByKey(_+_)
    lineCount.foreach(lineCount=>println(lineCount._1 + "appears " + lineCount._2 + " times."))

    sc.stop()
  }
}
