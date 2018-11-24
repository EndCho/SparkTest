package xiaoniu

import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object IpLocation2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 取到HDFS上的ip规则

    val ruleLines: RDD[String] = sc.textFile("C:\\Users\\txdyl\\Desktop\\log\\in\\ip.txt")
    val ipRulesRDD: RDD[(Long, Long, String)] = ruleLines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    })
    // 将分散在多个Executor中的部分IP规则收集到Driver端
    val rulesInDriver: Array[(Long, Long, String)] = ipRulesRDD.collect()

    // 将Driver端的数据广播到Executor
    // 广播变量的引用（还在Driver上）
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rulesInDriver)

    // 创建RDD，读取访问日志
    val accessLines: RDD[String] = sc.textFile("C:\\Users\\txdyl\\Desktop\\log\\in\\access.log")

    // 整理数据
    val provinceAndOne: RDD[(String, Int)] = accessLines.map(log=>{
      val fields = log.split("[|]")
      val ip = fields(1)
      // 将ip转换成十进制
      val ipNum = MyUtils.ip2Long(ip)
      // 进行十分法查找，通过Driver端的引用或取到Executor中的广播变量
      // (该函数中的代码是在Executor中被调用执行的，通过广播变量的引用，就可以拿到当前Executor中广播的规则了)
      // Driver端广播变量的引用是怎么跑到Executor中的呢？
      // Task是在Driver端中生成的，广播变量的引用是伴随着Task被发送到Executor中的
      // 闭包（相当于java中的内部类引用外部变量，外部变量也要传给内部类）
      val rulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value
      // 查找
      var province = "末知位置"
      val index = MyUtils.binarySeach(rulesInExecutor, ipNum)
      if (index != -1)
        province= rulesInExecutor(index)._3
      (province,1)
    })
    // 聚合
    val reduced: RDD[(String, Int)] = provinceAndOne.reduceByKey(_+_)
//    // 将结果打印
//    val result = reduced.collect()
//    println(result.toBuffer)


/*    reduced.foreach(tp=>{
      // 将数据写入到Mysql中
      // 在哪一端获取到Mysql的链接的？
      // 是在Executor中的Task获取的JDBC连接
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?charatorEncoding=utf-8","root","root")
      // 写入大量的数据的时候，有没问题？
      val pstm = conn.prepareStatement("....")
      pstm.setString(1,tp._1)
      pstm.setInt(2,tp._2)
      pstm.executeUpdate()
      pstm.close()
      conn.close()
    })*/
    // 一次拿一个分区(一个分区用一个连接，可以将一个分区中的多条数据写完再释放jdbc连接，这样更节省资源)
    reduced.foreachPartition(it=>MyUtils.data2MySQL(it))

    sc.stop()
  }
}
