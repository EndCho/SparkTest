package xiaoniu.customsort

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CustomSort6 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    //排序规则：首先按颜值进行排序，如果颜值相等，再按照年龄的升序
    val users = Array("laomaotao 90 89", "xiaobaicai 78 69", "lol 96 65", "dota 98 96", "Sony 88 65")

    // 将Driver的数据并行化变成RDD
    val lines: RDD[String] = sc.parallelize(users,2)

    // 切分整理数据
    val userRDD: RDD[(String, Int, Int)] = lines.map(line => {
      val fields = line.split(" ")
      val name = fields(0)
      val age = fields(1)
      val fv = fields(2)
      (name, age.toInt, fv.toInt)
    })


    // 不满足要求
    // tpRDD.sortBy(tp=>tp._3,false)

    // 排序（传入了一个排序规则，不会改变数据的格式，只会改变顺序）
    // 充分利用元组的比较规则，元组的比较规则：先比第一，相等再比第二个
    // Ordering[(Int,Int)]最终比较的规则格式
    // on[(String,Int,Int)]未比较之前的数据格式
    // (tp=>(-tp._3,tp._2))怎样将规则转换成想要比较的格式
    implicit val rules=Ordering[(Int,Int)].on[(String,Int,Int)](tp=>(-tp._3,tp._2))

    val sorted: RDD[(String, Int, Int)] = userRDD.sortBy(tp=>tp)
    val r = sorted.collect()
    print(r.toBuffer)
    sc.stop()

  }

}
