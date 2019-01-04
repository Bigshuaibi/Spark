package Spark002

import org.apache.spark.{SparkConf, SparkContext}


/**
  * @Author: 帅逼
  * @Description:
  * @Date: Create in 21:37 2019/1/2
  */
object sparkWordCount {
  def main(args: Array[String]): Unit = {
    //获取配置
    val conf = new SparkConf()
    conf.setAppName("sparkWordCount")
    conf.setMaster("local[2]")

    //获取context
    val context: SparkContext = new SparkContext(conf)

    val unit = context.textFile("hdfs://192.168.17.66:8020/wc")

    val collect = unit.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2,false)

    println(collect.collect().toBuffer)

  }

}
