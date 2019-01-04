package Spark002;

import org.apache.spark.SparkConf;
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
 * @Date: Create in 9:36 2019/1/2
 */

public class JavaWordCount {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        //2.0 之前不叫set
        //不写他会自动生成一个名字
        conf.setAppName("JavaWordCount");
        //两个线程
        conf.setMaster("local[2]");
        JavaSparkContext context = new JavaSparkContext(conf);

        // 获取数据
        JavaRDD<String> lines = context.textFile("hdfs://192.168.17.66:8020/wc");

        //切分 第一个泛型是输入的类型  第二个泛型是输出的类型  算子大都是 批处理的算子
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            //2.0 之前是 Iteratorable 类型的
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        //生成元组
        JavaPairRDD<String, Integer> tup =  words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        //聚合
        JavaPairRDD<String, Integer> agged = tup.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        //排序 spark中并没有在JavaApi中提供SortBy算子
        JavaPairRDD<Integer, String> swap = agged.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {

            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> s) throws Exception {
                return s.swap();
            }
        });

        JavaPairRDD<Integer, String> sort = swap.sortByKey(false);

        //
        JavaPairRDD<String, Integer> rdd = sort.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> s) throws Exception {
                return s.swap();
            }
        });

        System.out.println(rdd.collect());


    }

}
