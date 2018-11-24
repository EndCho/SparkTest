package cn.rzlee.spark.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SecondSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass().getSimpleName).setMaster("local")
    val sc = new SparkContext(conf)


    val lines: RDD[String] = sc.textFile("C:\\Users\\txdyl\\Desktop\\log\\in\\sort.txt",1)
    val pairs = lines.map(line => {
      (new SecondarySortKey(line.split("\t")(0).toInt, line.split("\t")(1).toInt),line)
    })
    val sortedPairs: RDD[(SecondarySortKey, String)] = pairs.sortByKey()
    val sortedLines: RDD[String] = sortedPairs.map(sortPair =>sortPair._2)

    sortedLines.foreach(line=>println(line))


    
  }
}
