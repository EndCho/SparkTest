package cn.rzlee.spark.core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * @Author ^_^
 * @Create 2018/11/3
 */
public class AccumulatorVariable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("AccumulatorVariable").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建Accumulator变量
        // 需要调用SparkContext的accumulator()方法
        Accumulator<Integer> sum = sc.accumulator(0);

        List<Integer> numbersList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 9, 8);
        JavaRDD<Integer> numbers = sc.parallelize(numbersList);

        numbers.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                // 然后在函数内部，就可以对Accumulator变量，调用add()方法，累加值
                sum.add(integer);
            }
        });
        // 在driver程序中，可以调用accumulator的value()方法，获取其值
        System.out.println(sum.value());

    }
}
