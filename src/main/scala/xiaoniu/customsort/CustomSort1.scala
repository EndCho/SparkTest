package xiaoniu.customsort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CustomSort1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    val sc = new SparkContext(conf)

    //排序规则：首先按颜值进行排序，如果颜值相等，再按照年龄的升序
    val users = Array("laomaotao 90 89", "xiaobaicai 78 69", "lol 96 65", "dota 98 96", "Sony 88 65")

    // 将Driver的数据并行化变成RDD
    val lines: RDD[String] = sc.parallelize(users,2)

    // 切分整理数据
    val userRDD: RDD[User] = lines.map(line => {
      val fields = line.split(" ")
      val name = fields(0)
      val age = fields(1)
      val fv = fields(2)
      new User(name, age.toInt, fv.toInt)
    })

    //不满足要求
    //tpRDD.sortBy(tp=>tp._3,false)

    // 将RDD里面装的User类型的数据进行排序
    val sorted: RDD[User] = userRDD.sortBy(u=>u)
    val r = sorted.collect()
    print(r.toBuffer)
    sc.stop()
  }
}


class User(val name: String, val age: Int, val fv: Int) extends Ordered[User] with Serializable {
  override def compare(that: User): Int = {
    if(this.fv == that.fv){
      this.age-that.age
    }else{
      -(this.fv-that.fv)
    }
  }

  override def toString: String = s" name: $name age: $age fv: $fv"
}