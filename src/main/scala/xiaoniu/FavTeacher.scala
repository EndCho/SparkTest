package xiaoniu

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FavTeacher {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass().getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)


    //指定以后从哪里读取数据
    val lines:RDD[String] = sc.textFile("C:\\Users\\txdyl\\Desktop\\log\\in\\weblog.txt")
    //整理数据

    val teacherAndOne: RDD[(String, Int)] = lines.map(line => {

      val index: Int = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      val httpHost = line.substring(0, index)
      val subject = new URL(httpHost).getHost.split("[.]")(0)

      (teacher, 1)
    })
    // 聚合
    val reduced: RDD[(String, Int)] = teacherAndOne.reduceByKey(_+_)
    // 排序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2,false)
    // 触发Action执行计算
    val result: Array[(String, Int)] = sorted.collect()
    // 打印
    println(result.toBuffer)

    sc.stop()

  }

}
