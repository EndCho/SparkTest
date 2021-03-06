package xiaoniu

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object GroupFavTeacher4 {
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


    // 计算有多少学科
    val subjects: Array[String] = subjectAndTeacher.map(_._1._1).distinct().collect()

    // 自定义一个分区器，并且按照指定的分区器进行分区
    val sbPartitioner: SubjectPartitioner2 = new SubjectPartitioner2(subjects)


    // 聚合，聚合就是按照指定的分区器进行分区
    // 该RDD一个分区内仅有一个学科的数据
    val reduced: RDD[((String, String), Int)] = subjectAndTeacher.reduceByKey(sbPartitioner,_+_)





    // partitionBy按照指定的分区规则进行分区
    val partitoned: RDD[((String, String), Int)] = reduced.partitionBy(sbPartitioner)

    // 如果一次拿出一个分区（可以操作一个分区中的数据了）
    val sorted: RDD[((String, String), Int)] = partitoned.mapPartitions(it => {
      // 将迭代器转换成list，然后排序，再转换成迭代器返回
      it.toList.sortBy(_._2).take(3).iterator
    })

    // 收集结果
    val result: Array[((String, String), Int)] = sorted.collect()

    println(result.toBuffer)

    sc.stop()
  }

}

// 自定义分区器
class SubjectPartitioner2(sbs: Array[String]) extends Partitioner{

  // 相当于主构造器（new 的时候会执行一次）
  // 用于存放规则的一个map
  val rules = new mutable.HashMap[String, Int]()
  var i = 0
  for (sb <-sbs ){
    rules.put(sb,i)
    i += 1
  }
  // 返回分区的数量（下一个RDD有多少分区）
  override def numPartitions: Int = sbs.length
  // 根据传入的key计算分区标号
  // key是一个元组(String, String)
  override def getPartition(key: Any): Int = {
    // 获取学科名称
    val subject = key.asInstanceOf[(String, String)]._1
    // 根据规则计算分区编号
    rules(subject)
  }
}
