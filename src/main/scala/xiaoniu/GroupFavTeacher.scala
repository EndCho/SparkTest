package xiaoniu

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GroupFavTeacher {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass().getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)


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
    

    // 和一组合在一起(不好，调用了两次map)
    // val map: RDD[((String, String), Int)] = subjectAndTeacher.map((_,1))

    // 聚合，将学科和老师联合当作key
    val reduced: RDD[((String, String), Int)] = subjectAndTeacher.reduceByKey(_+_)
    // 分组排序（按学科进行排序）
    // 【学科，该学科对应的老师的数据】
    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)
    // 经过分组后，一个分区内可能有多个学科的数据，一个学科就是一个迭代器
    // 将每个组拿出来进行操作
    // 为什么可以调用Scala的sortby方法呢？因为一个学科的数据已经在同一台机器上的一个scala集合里面了
    val sorted: RDD[(String, List[((String, String), Int)])] = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(3))

    val result: Array[(String, List[((String, String), Int)])] = sorted.collect()

    reduced.map(x=>x)
    print(result.toBuffer)
    sc.stop()

  }

}
