package cn.rzlee.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import scala.actors.threadpool.Arrays;

/**
 * @Author ^_^
 * @Create 2018/11/3
 */
public class SortWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SortWordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建line RDD
        JavaRDD<String> lines = sc.textFile("C:\\Users\\txdyl\\Desktop\\log\\in\\data.txt", 1);

        // 执行单词计数
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split("\t"));
            }
        });


        JavaPairRDD<String, Integer> pair = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> wordCounts = pair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 进行key-value的反转映射
        JavaPairRDD<Integer, String> countWords = wordCounts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<>(t._2, t._1);
            }
        });

        // 按照key进行排序
        JavaPairRDD<Integer, String> sortedCountWords = countWords.sortByKey(false);

        // 再次进行key-value的反转映射
        JavaPairRDD<String, Integer> sortedWordCounts = sortedCountWords.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                return new Tuple2<>(t._2, t._1);
            }
        });


        // 打印结果
        sortedWordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + " appears " + t._2+ " times.");
            }
        });
        // 关闭JavaSparkContext
        sc.close();
    }
}
