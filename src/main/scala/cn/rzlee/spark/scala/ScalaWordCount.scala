package cn.rzlee.spark.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCount {
  def main(args: Array[String]): Unit = {

    //创建spark配置,设置应用程序名字
    val conf = new SparkConf().setMaster("local[1]")
      .setAppName("wordCountApp")

    // 创建spark执行入口
    val sc = new SparkContext(conf)

    // 指定以后从哪里读取数据创建RDD（弹性分布式数据集）
    val lines: RDD[String] = sc.textFile("C:\\Users\\txdyl\\Desktop\\log\\in\\data.txt")
    // 切分压平
    val words: RDD[String] = lines.flatMap(_.split(" "))
    // 将单词和一组合
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))
    // 按key进行聚合  相同key不变，将value相加
    val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)
    // 排序
    val sorted = reduced.sortBy(_._2,false)
    // 将结果保存到HDFS中
    sorted.saveAsTextFile("C:\\Users\\txdyl\\Desktop\\log\\out")
    //释放资源
    sc.stop()
  }
}

