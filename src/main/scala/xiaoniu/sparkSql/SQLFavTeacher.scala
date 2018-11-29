package xiaoniu.sparkSql

import java.net.URL

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}





object SQLFavTeacher {
  def main(args: Array[String]): Unit = {

    val topN=args(0).toInt

    val spark = SparkSession.builder().
      appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()

    val lines: Dataset[String] = spark.read.textFile(args(1))

    import spark.implicits._

    val df: DataFrame = lines.map(line => {
      val tIndex = line.lastIndexOf("/") + 1
      val teacher = line.substring(tIndex)
      val host = new URL(line).getHost
      // 学科的Index
      val sIndex = host.indexOf(".")
      val subject = host.substring(0, sIndex)

      (subject, teacher)
    }).toDF("subject", "teacher")

    df.createTempView("v_sub_teacher")

    // 该学科下的老师的访问次数
    val temp1: DataFrame = spark.sql("SELECT subject, teacher, count(*) counts FROM v_sub_teacher group by subject, teacher")

    // 求每个学科下最受欢迎的老师的topn
    temp1.createTempView("v_temp_sub_teacher_counts")

    val temp2: DataFrame = spark.sql(s"select * from (select subject, teacher, counts, row_number() over(partition by subject order by counts desc) sub_rk, row_number() over(order by counts desc) g_rk  from v_temp_sub_teacher_counts) tmp where sub_rk<=$topN")
    temp2.show()

    spark.close()

  }
}
