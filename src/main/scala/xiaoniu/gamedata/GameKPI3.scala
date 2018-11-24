package xiaoniu.gamedata

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GameKPI3 {
  def main(args: Array[String]): Unit = {
    // 2018-11-20
    val startDate = args(0)
    // 2018-11-21
    val endDate = args(1)

    // 查询条件
    val dateFormat1 = new SimpleDateFormat("yyyy-MM-dd")

    // 查询条件的起始时间
    val startTime = dateFormat1.parse(startDate).getTime
    // 查询条件的截止时间
    val endTime = dateFormat1.parse(endDate).getTime

    // Driver端定义的一个simpleDateFormat
    val dateFormat2 = new SimpleDateFormat("yyyy年MM月dd日,E,HH:mm:ss")

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 以后从哪里读数据
    val lines: RDD[String] = sc.textFile(args(2))
    // 整理并过滤
    val splited: RDD[Array[String]] = lines.map(line=>line.split("[|]"))

    // 按日期进行过滤
//    val filteredByType: RDD[Array[String]] = splited.filter(fields => {
//      // 一个task中会创建多个
//      val fu = new FilterUtilsV3
//      fu.filterByType(fields, "1")
//    })


    val filtered = splited.filter(fields => {
      // 一个task中会创建多个
      val fu = new FilterUtilsV3
      fu.filterByTime(fields, startTime, endTime)
    })
    val dnu = filtered.count()

    println(dnu)
  }

}
