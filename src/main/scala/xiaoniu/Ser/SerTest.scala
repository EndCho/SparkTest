package xiaoniu.Ser

import java.net.InetAddress
import xiaoniu.Ser

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SerTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("C:\\Users\\txdyl\\Desktop\\log\\in\\bigdata.txt")

    val r = lines.map(word => {
      // 在map的函数中，创建一个rules实例（太浪费资源）
      val rules = new Rules

      val hostName = InetAddress.getLocalHost.getHostName
      val threadName = Thread.currentThread().getName
      (hostName, threadName, rules.rulesMap.getOrElse(word,0), rules.toString)
    })
    r.saveAsTextFile("C:\\Users\\txdyl\\Desktop\\log\\out\\bigdata")
  }

}
