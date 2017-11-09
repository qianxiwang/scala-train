package com.aviup.1108

import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created with IntelliJ IDEA.
  * Description: 计数器的使用，使用计数器完成你的Spark作业的处理数据量
  * Author: lihongjie
  * Date: 2017-11-08
  * Time: 下午5:55
  */
object AccumulatorsApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("AccumulatorsApp")
    val sparkContext = new SparkContext(sparkConf)
    val rdd = sparkContext.textFile("/Users/lihongjie/data/emp.txt")
    val errorAccum = sparkContext.longAccumulator("ErrorAccumulator")
    val newData = rdd.filter(line => {
      val splits = line.split("\t")
      var benefit = ""
      try {
        benefit = splits(6)
        if ("".equals(benefit)) {
          errorAccum.add(1)
        }
      } catch {
        case e: Exception => errorAccum.add(1)
      }
      !"".equals(benefit)
    })
    //如果经过两次action，累加器会执行两次（要么cache RDD，要么只做一次action）
    newData.cache
    //津贴不为空的条数
    val count = newData.count()
    newData.foreach(println)
    DAO.SaveAccumulatorToMySQL("AccumulatorsApp", errorAccum.value, count)
  }
}
