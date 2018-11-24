package xiaoniu.sparkSql

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object DataSetWordCount {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    // （指定以后从哪里）读数据，是lazy
    //  DateSet分布式数据集，是对RDD的进一步封装，是更加智能的RDD
    // DataSet只有一列，默认这列叫value
    val lines: Dataset[String] = spark.read.textFile("C:\\Users\\txdyl\\Desktop\\log\\in\\data.txt")
    // 整理数据(切分压平)
    // 导入隐式转换
    import spark.implicits._
    val words: Dataset[String] = lines.flatMap(_.split("\t"))

    // 使用DataSet的API（DSL）
    // val count: DataFrame = words.groupBy($"value" as "word").count().sort($"count" desc)
    import org.apache.spark.sql.functions._
    val counts: Dataset[Row] = words.groupBy($"value" as "word").agg(count("*") as "counts").orderBy($"counts" desc)
    counts.show()
    spark.close()
  }
}
