package xiaoniu

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object FavTeacher2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass().getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    val subject =Array("bigdata","java","php")

    //指定以后从哪里读取数据
    val lines:RDD[String] = sc.textFile("C:\\Users\\txdyl\\Desktop\\log\\in\\weblog.txt")
    //整理数据
    val subjectAndTeacher: RDD[((String, String), Int)] = lines.map(line => {

      val index: Int = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      val httpHost = line.substring(0, index)
      val subject = new URL(httpHost).getHost.split("[.]")(0)
      ((subject, teacher), 1)
    })

    // 聚合，将学科和老师联合当作key
    val reduced: RDD[((String, String), Int)] = subjectAndTeacher.reduceByKey(_+_)

    // 聚合后不分组，分组的话数据量很多开销很大
    // Scala的集合排序是在内存中进行的，但是内存有可能不够用
    // 可以调用RDD的sortBy方法，内存+磁盘进行排序
    for (sb <- subject){
      // 该RDD中对应的数据仅有一个学科的数据（因为过滤了）
      val filtered: RDD[((String, String), Int)] = reduced.filter(_._1._1 == sb)

      // 现在调用的是RDD的sortBy方法,(take是一个action，会触发任务提交)
      val favTeacher: Array[((String, String), Int)] = filtered.sortBy(_._2,false).take(3)
      // 打印
      print(favTeacher.toBuffer)
    }

    sc.stop()
  }
}
