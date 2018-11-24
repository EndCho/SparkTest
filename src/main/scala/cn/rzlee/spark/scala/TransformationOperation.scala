package cn.rzlee.spark.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TransformationOperation {
  def main(args: Array[String]): Unit = {

  //map()
  //filter()
  //flatMap()
    // groupByKey()
  //reduceByKey()
    //sortByKey()
    //join()
    cogroup()
  }


  // 将集合中每个元素乘以2
  def map(){
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[1]")
    val sc = new SparkContext(conf)

    val numbers = Array(1,2,3,4,5)
    val numberRDD: RDD[Int] = sc.parallelize(numbers,1)
    numberRDD.foreach(num=>println(num))

  }
  
  // 过滤出集合中的偶数
  def filter(): Unit ={
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[1]")
    val sc = new SparkContext(conf)

    val numbers = Array(1,2,3,4,5)
    val numberRDD: RDD[Int] = sc.parallelize(numbers,1)
    val evenNumbersRdd = numberRDD.filter(num=>num%2==0)
    evenNumbersRdd.foreach(num=>println(num))
  }


  // 将行拆分为单词
  def flatMap(): Unit ={
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[1]")
    val sc = new SparkContext(conf)


    val lineArray = Array("hello you", "just do it", "go go go")
    val lines = sc.parallelize(lineArray, 1)
    val words: RDD[String] = lines.flatMap(line=>line.split(" "))
    words.foreach(word=>println(word))
  }


  // 将每个班级的成绩进行分组
  def groupByKey(): Unit ={
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[1]")
    val sc = new SparkContext(conf)
    val scoresList = Array(Tuple2("class1", 50), Tuple2("class1", 95), Tuple2("class2", 60), Tuple2("class2", 88))
    val scores: RDD[(String, Int)] = sc.parallelize(scoresList, 1)
    val groupedScoreds = scores.groupByKey()
    groupedScoreds.foreach(scored=>{
      println(scored._1)
      scored._2.foreach(singleScore=>println(singleScore))
      println("=====================================")
    })
  }

  // 统计每个班级的总分
  def reduceByKey(): Unit ={
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[1]")
    val sc = new SparkContext(conf)

    val scoresList = Array(Tuple2("class1", 50), Tuple2("class1", 95), Tuple2("class2", 60), Tuple2("class2", 88))
    val scores: RDD[(String, Int)] = sc.parallelize(scoresList, 1)
    val totalScores: RDD[(String, Int)] = scores.reduceByKey(_+_)
    totalScores.foreach(totalScore=>println(totalScore._1 +" : " + totalScore._2))

  }




  //将学生分数进行排序
  def sortByKey(): Unit ={
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[1]")
    val sc = new SparkContext(conf)
    val scoreList = Array(Tuple2(90,"leo"), Tuple2(99, "kent"), Tuple2(80,"Jeo"), Tuple2(91,"Ben"), Tuple2(96,"Sam"))
    val scores: RDD[( Int,String)] = sc.parallelize(scoreList, 1)
    val sortedScores = scores.sortByKey(false)
    sortedScores.foreach(student=>println(student._2 +" : " + student._1))
  }

  // 打印每个学生的成绩
  def join(): Unit ={
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[1]")
    val sc = new SparkContext(conf)
    
    val studentsList = Array(Tuple2(1,"leo"), Tuple2(2, "Sam"), Tuple2(3, "kevin"))
    val scoresList = Array(Tuple2(1,60), Tuple2(2,70), Tuple2(3,80))

    val students: RDD[(Int, String)] = sc.parallelize(studentsList,1)
    val scores: RDD[(Int, Int)] = sc.parallelize(scoresList,1)
    val studentScores: RDD[(Int, (String, Int))] = students.join(scores)
    studentScores.foreach(studentScore=>{
      println("studentid: "+studentScore._1)
      println("studentNmae:"+studentScore._2._1)
      println("studentScore: "+ studentScore._2._2)
      println("###################################################")
    })
  }
  
  // 打印每个学生的成绩
  // cogroup相当于full join
  def cogroup(): Unit ={
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[1]")
    val sc = new SparkContext(conf)

    val studentsList = Array(Tuple2(1,"leo"), Tuple2(2, "Sam"), Tuple2(3, "kevin"))
    val scoresList = Array(Tuple2(1,60), Tuple2(2,70), Tuple2(3,80))

    val students: RDD[(Int, String)] = sc.parallelize(studentsList,1)
    val scores: RDD[(Int, Int)] = sc.parallelize(scoresList,1)

    val studentScores: RDD[(Int, (Iterable[String], Iterable[Int]))] = students.cogroup(scores)
    studentScores.foreach(studentScore =>{
      println("studentid: " + studentScore._1)
      println("studentname: "+ studentScore._2._1)
      println("studentscore: "+ studentScore._2._2)

    })
    
  }

  
}
