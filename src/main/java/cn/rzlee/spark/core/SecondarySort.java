package cn.rzlee.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * @Author ^_^
 * @Create 2018/11/3
 * 二次排序
 */
public class SecondarySort {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SecondarySort").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> lines = sc.textFile("C:\\Users\\txdyl\\Desktop\\log\\in\\sort.txt", 1);
        JavaPairRDD<SecondarySortByKey, String> pairs = lines.mapToPair(new PairFunction<String, SecondarySortByKey, String>() {
            @Override
            public Tuple2<SecondarySortByKey, String> call(String line) throws Exception {
                String[] lineSplited = line.split("\t");

                SecondarySortByKey key = new SecondarySortByKey(Integer.valueOf(lineSplited[0]), Integer.valueOf(lineSplited[1]));
                return new Tuple2<>(key, line);
            }
        });
        System.out.println(pairs.collect());
        JavaPairRDD<SecondarySortByKey, String> sortedPairs = pairs.sortByKey();
        System.out.println(sortedPairs.collect());
        JavaRDD<String> sortedLines = sortedPairs.map(new Function<Tuple2<SecondarySortByKey, String>, String>() {
            @Override
            public String call(Tuple2<SecondarySortByKey, String> v1) throws Exception {
                return v1._2;
            }
        });
        sortedLines.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });


        sc.close();

    }
}
