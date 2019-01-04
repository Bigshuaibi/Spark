package spark003

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: 帅逼
  * @Description:
  * @Date: Create in 22:19 2019/1/3 
  */
object Text {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Text")
    val sc  = new SparkContext(conf)
    val rdd3 = sc.parallelize(List("12","23","345","4567"),2)
    rdd3.aggregate("")((x,y) => math.max(x.length, y.length).toString, (x,y) => x + y)
  }

}
