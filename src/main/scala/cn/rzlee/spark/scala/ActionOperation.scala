package cn.rzlee.spark.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ActionOperation {
  def main(args: Array[String]): Unit = {
    //reduce()
    //collect()
    //count()
    //take()
    //saveAsTextFile()
    countByKey()
  }


  def reduce(): Unit ={
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[1]")
    val sc = new SparkContext(conf)

    val numbersList = Array(1,2,3,4,5,6,7,8,9,10)
    val numbersRdd: RDD[Int] = sc.parallelize(numbersList,1)
    val sum: Int = numbersRdd.reduce(_+_)
    println(sum)
  }


  def collect(): Unit ={
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[1]")
    val sc = new SparkContext(conf)

    val numbersList = Array(1,2,3,4,5,6,7,8,9,10)
    val numbersRdd: RDD[Int] = sc.parallelize(numbersList,1)

    val doubleNumbers: RDD[Int] = numbersRdd.map(num=>num*2)
    for(num <- doubleNumbers){
      println(num)
    }
  }

  def count(): Unit ={
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[1]")
    val sc = new SparkContext(conf)

    val numbersList = Array(1,2,3,4,5,6,7,8,9,10)
    val numbersRdd: RDD[Int] = sc.parallelize(numbersList,1)
    val count: Long = numbersRdd.count()
    println(count)
  }




  def take(): Unit ={
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[1]")
    val sc = new SparkContext(conf)

    val numbersList = Array(1,2,3,4,5,6,7,8,9,10)
    val numbersRdd: RDD[Int] = sc.parallelize(numbersList,1)

    val top3Numners = numbersRdd.take(3)
    for (num <- top3Numners){
      println(num)
    }
  }

  def saveAsTextFile(): Unit ={
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[1]")
    val sc = new SparkContext(conf)

    val numbersList = Array(1,2,3,4,5,6,7,8,9,10)
    val numbersRdd: RDD[Int] = sc.parallelize(numbersList,1)
    numbersRdd.saveAsTextFile("C:\\Users\\txdyl\\Desktop\\log\\out\\saveAsTest\\")
  }

  def countByKey(): Unit ={
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[1]")
    val sc = new SparkContext(conf)

    val studentList = Array(Tuple2("class1","tom"),Tuple2("class2","leo"), Tuple2("class1","jeo"),Tuple2("class2","jime"))
    val students: RDD[(String, String)] = sc.parallelize(studentList, 1)
    val studentsCounts: collection.Map[String, Long] = students.countByKey()
    println(studentsCounts)
  }

  // foreach是在远程机器上执行的，而不是将数据拉取到本地一条条执行，所以性能要比collect要高很多。

}
