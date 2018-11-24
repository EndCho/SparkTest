package xiaoniu.Ser

import java.net.InetAddress

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 如果在Driver端定义一个类，返回new一个实例，并且RDD的函数引用了该实例，
  * 那么会伴随着Task发送过去，并且每个task中都会有一份单独的实例
  */


object SerTest1 {
  def main(args: Array[String]): Unit = {
    // 在Driver端被生成
    val rules = new Rules
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)



    val lines: RDD[String] = sc.textFile("C:\\Users\\txdyl\\Desktop\\log\\in\\bigdata.txt")

    val r = lines.map(word => {
      val hostName = InetAddress.getLocalHost.getHostName
      val threadName = Thread.currentThread().getName
      // rules的实际使用是在Executor中使用的
      (hostName, threadName, rules.rulesMap.getOrElse(word,0), rules.toString)
    })
    r.saveAsTextFile("C:\\Users\\txdyl\\Desktop\\log\\out\\bigdata")
  }

}
