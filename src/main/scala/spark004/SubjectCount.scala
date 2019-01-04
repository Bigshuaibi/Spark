package spark004

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: 帅逼
  * @Description:
  * @Date: Create in 11:17 2019/1/4 
  */
object SubjectCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SubjectCount").setMaster("local")
    val sc = new SparkContext(conf)
    //获取数据
    val logs = sc.textFile("E:/spark/Spark02/sparkcoursesinfo/spark/data/subjectaccess")
    //切分
    val url: RDD[String] = logs.map(line => line.split("\t")(1))
    val tup: RDD[(String, Int)] = url.map((_, 1))

    val reduced: RDD[(String, Int)] = tup.reduceByKey(_ + _)
    val maped: RDD[(String, (String, Int))] = reduced.map(x => {
      val strings = new URL(x._1).getHost
     // println(strings)

      val str: String = x._1.split("\\.")(0).split("//")(1)
      //学科名
      (str, (x._1, x._2))

    })
    val groupbyKeyed: RDD[(String, Iterable[(String, Int)])] = maped.groupByKey()
    val res: RDD[(String, List[(String, Int)])] = groupbyKeyed.mapValues(_.toList.sortBy(_._2).reverse)
    val ss = res.mapValues(_.take(2))

    print(ss.collect().toBuffer)

  }

}
