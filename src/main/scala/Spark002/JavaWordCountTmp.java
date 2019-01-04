package Spark002;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @Author: 帅逼
 * @Description:
 * @Date: Create in 20:52 2019/1/2
 */

public class JavaWordCountTmp {
    public static void main(String[] args) {
        //配置
        SparkConf conf = new SparkConf();
        //获取context
        conf.setAppName("JavaWordCountTmp");
        conf.setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<String> lines = jsc.textFile("hdfs://192.168.17.66:8020/wc");

        //切分 第一个是 输入类型 第二个是输出类型
        JavaRDD<String> splited = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        //生成元组
        JavaPairRDD<String, Integer> tup = splited.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        //聚合
        JavaPairRDD<String, Integer> rdd = tup.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //排序
        JavaPairRDD<Integer, String> swaped = rdd.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> s) throws Exception {
                return s.swap();
            }
        });
        //排序
        JavaPairRDD<Integer, String> sorted = swaped.sortByKey(false);//降序排序

        JavaPairRDD<String, Integer> rdd1 = sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> s) throws Exception {
                return s.swap();
            }
        });

        System.out.println( rdd1.collect());
    }

}
