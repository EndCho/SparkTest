package cn.rzlee.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @Author ^_^
 * @Create 2018/11/3
 */
public class Persist {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Persist").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("C:\\Users\\txdyl\\Desktop\\log\\in\\data.txt", 1).cache();
        long beginTime = System.currentTimeMillis();
        long count = lines.count();
        System.out.println(count);
        long endTime = System.currentTimeMillis();
        System.out.println("cost "+(endTime - beginTime) + "millisecond");


        beginTime = System.currentTimeMillis();
        count = lines.count();
        System.out.println(count);
        endTime = System.currentTimeMillis();
        System.out.println("cost "+(endTime - beginTime) + "millisecond");

        sc.close();
    }
}
