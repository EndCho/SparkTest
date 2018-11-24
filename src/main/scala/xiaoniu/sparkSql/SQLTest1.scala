package xiaoniu.sparkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types._

object SQLTest1 {
  def main(args: Array[String]): Unit = {
    //  spark2.x SQL的编程API(SparkSession)
    // 是spark2.x SQL执行的入口
    val session = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[2]").getOrCreate()

    // 创建RDD
    val lines: RDD[String] = session.sparkContext.textFile("C:\\Users\\txdyl\\Desktop\\log\\in\\person.txt")


    // 将数据进行整理
    val rowRDD: RDD[Row] = lines.map(line => {
      val fields: Array[String] = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toDouble
      Row(id, name, age, fv)
    })

    // 结果类型，其实就是表头，用于描述DataFrame
    val schema: StructType = StructType(List(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("fv", DoubleType, true)
    ))

    // 创建DataFrame
    val df: DataFrame = session.createDataFrame(rowRDD, schema)
    import session.implicits._
    val df1: Dataset[Row] = df.where($"fv">98).orderBy($"fv" desc, $"age" asc)
    df1.show()
    session.stop()
  }

}
