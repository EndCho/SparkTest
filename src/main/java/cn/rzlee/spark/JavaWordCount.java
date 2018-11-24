package cn.rzlee.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.actors.threadpool.Arrays;

/**
 * @Author ^_^
 * @Create 2018/9/2
 */
public class JavaWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JavaWordCount");
        // 创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        // 指定以后从哪里读取数据
        JavaRDD<String> lines = jsc.textFile(args[0]);
        // 切分压平
        JavaRDD<String> word = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });
    }
}
