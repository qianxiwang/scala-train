package com.aviup

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
      val sparkConf=new SparkConf().setMaster("local[2]").setAppName("AccumulatorsApp")
      val sparkContext=new SparkContext(sparkConf)
      val rdd=sparkContext.textFile("/Users/lihongjie/data/accumulators.txt")
      val accum = sparkContext.longAccumulator("My Accumulator")
      rdd.flatMap(
        x=>{accum.add(1)
        x.split("\t")})
        .map((_, 1)).reduceByKey(_+_)
      DAO.SaveAccumulatorToMySQL("AccumulatorsApp",accum.value)
    }
}
