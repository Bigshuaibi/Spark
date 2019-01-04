package Spark002;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * @Author: 帅逼
 * @Description:
 * @Date: Create in 10:11 2019/1/2
 */

public class JavaLamadaWC {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JavaLamadaWC").setMaster("local[2]");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> lines = jsc.textFile("hdfs://192.168.17.66:8020/wc");

         JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());


    }

}
