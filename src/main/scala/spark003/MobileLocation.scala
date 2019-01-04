package spark003

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author: 帅逼
  * @Description:
  * @Date: Create in 17:43 2019/1/3
  */
object MobileLocation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MobileLocation").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("E:/spark/Spark02/sparkcoursesinfo/spark/data/lacduration/log")
    val splited = lines.map(x => {
      val fields = x.split(",")
      val phoneNum = fields(0) //手机号
      val time = fields(1).toLong //时间戳
      val lac = fields(2) //基站ID
      val eventType = fields(3) //事件类型
      ((phoneNum, lac), time)


    })

    //计算用户在基站挺溜的总时长
    val timeall = splited.reduceByKey(_ + _)


    //获取基站的信息
    val ls = sc.textFile("E:/spark/Spark02/sparkcoursesinfo/spark/data/lacduration/lac_info.txt")

    //切分基站的信息

    val splitedLac = ls.map(z => {
      val fields = z.split(",")
      val lacID = fields(0) //基站ID
      val x = fields(1)
      //经度
      val y = fields(2) //维度
      (lacID, (x, y))

    })

    //为了方便基站进行join 对用户数据进行整理
    val maped = timeall.map(x => {

      (x._1._2, (x._1._1, x._2))
    })
    //聚合
    val joined: RDD[(String, ((String, Long), (String, String)))] = maped.join(splitedLac)
    //整合数据
    val maped1 = joined.map(x => {


      (x._1, x._2._1._1, x._2._1._2, x._2._2)
    })
    val groupByed: RDD[(String, Iterable[(String, String, Long, (String, String))])] = maped1.groupBy(x => x._2)
    //按用户进行分组


    //按照时长进行排序

    val sorted=groupByed.mapValues(_.toList.sortBy(_._3))

    //取 top2
    val res = sorted.mapValues(_.take(2))

    //输出
    print(res.collect().toBuffer)

  }

}
