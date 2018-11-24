package xiaoniu

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IpLocation1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)


    // 在Driver端获取到全部的IP规则数据（全部的规则数据在某一台机器上，跟Driver在同一台机器上）
    // 全部的IP规则在Driver端了（在Driver端的内存里了）
    val rules: Array[(Long, Long, String)] = MyUtils.readRules("C:\\Users\\txdyl\\Desktop\\log\\in\\ip.txt")

    // 调用sc上的广播方法
    // 广播变量的引用（还在Driver上）
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rules)

    // 创建RDD，读取访问日志
    val accessLines: RDD[String] = sc.textFile("C:\\Users\\txdyl\\Desktop\\log\\in\\access.log")

    // 这个函数是在哪一端定义的？ （Driver）
    val func = (line:String)=>{
      val fields = line.split("[|]")
      val ip = fields(1)
      // 将ip转换成十进制
      val ipNum = MyUtils.ip2Long(ip)
      // 进行十分法查找，通过Driver端的引用或取到Executor中的广播变量
      // (该函数中的代码是在Executor中被调用执行的，通过广播变量的引用，就可以拿到当前Executor中广播的规则了)
      val rulesOnExecutor: Array[(Long, Long, String)] = broadcastRef.value
      // 查找
      var province = "末知位置"
      val index = MyUtils.binarySeach(rulesOnExecutor, ipNum)
      if (index != -1)
        province= rulesOnExecutor(index)._3
      (province,1)
    }
    // 整理数据
    val provinceAndOne: RDD[(String, Int)] = accessLines.map(func)
    // 聚合
    val reduced: RDD[(String, Int)] = provinceAndOne.reduceByKey(_+_)
    // 将结果打印
    val result = reduced.collect()
    println(result.toBuffer)
    sc.stop()
  }

}
