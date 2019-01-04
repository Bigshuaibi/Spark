package spark004

import java.net.URL

import org.apache.commons.lang.mutable.Mutable
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @Author: 帅逼
  * @Description:
  * @Date: Create in 11:17 2019/1/4 
  */
object SubjectCount_3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SubjectCount").setMaster("local")
    val sc = new SparkContext(conf)
    //获取数据
    val logs = sc.textFile("E:/spark/Spark02/sparkcoursesinfo/spark/data/subjectaccess")
    //切分
    val url: RDD[String] = logs.map(line => line.split("\t")(1))
    val tup: RDD[(String, Int)] = url.map((_, 1))

    val reduced: RDD[(String, Int)] = tup.reduceByKey(_ + _).cache()

    //获取学科信息

    val subjectInfo = reduced.map(x => {
      val url = x._1
      val count = x._2
      val subject = new URL(url).getHost
      (subject, (url, count))
    }).cache()

    //    val partitioned = subjectInfo.partitionBy(new HashPartitioner(3))
    //    partitioned.saveAsTextFile("e:/output")

    val subjects: Array[String] = subjectInfo.keys.distinct.collect()

    val partition: SubjectPartition = new SubjectPartition(subjects)

    val partitioned = subjectInfo.partitionBy(partition)
    partitioned.saveAsTextFile("e:/output")

    /**
      * distinct  源码去重
      * def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] = withScope {
      * map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)
      * }
      */
    sc.stop()
  }

}

class SubjectPartition(val subjects: Array[String]) extends Partitioner {
  val sunjectAndPartition = new mutable.HashMap[String, Int]()

  for (sunject <- subjects) {
    val i = sunjectAndPartition.size
    sunjectAndPartition += (sunject -> i)

  }
  //分区数
  override def numPartitions: Int = subjects.length





  //分区号
  override def getPartition(key: Any): Int = {
     sunjectAndPartition.getOrElse(key.toString, 0)


  }
}