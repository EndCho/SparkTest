package cn.rzlee.spark.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local")
    val sc = new SparkContext(conf)
    
    
    val lines = sc.textFile("C:\\Users\\txdyl\\Desktop\\log\\in\\data.txt",1)
    val words: RDD[String] = lines.flatMap(line=>line.split("\t"))
    val pairs: RDD[(String, Int)] = words.map(word=>(word,1))
    val wordCounts: RDD[(String, Int)] = pairs.reduceByKey(_+_)
    val countWords: RDD[(Int, String)] = wordCounts.map(wordCount=>(wordCount._2, wordCount._1))
    val sortedCountWords = countWords.sortByKey(false)
    val sortedWordCounts: RDD[(String, Int)] = sortedCountWords.map(sortedCountWord=>(sortedCountWord._2, sortedCountWord._1))
    sortedWordCounts.foreach(sortedWordCount=>{
      println(sortedWordCount._1+" appear "+ sortedWordCount._2 + " times.")
    })

    sc.stop()
  }

}
