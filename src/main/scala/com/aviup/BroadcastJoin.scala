package com.aviup

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA.
  * Description: 广播变量的使用
  * Author: lihongjie
  * Date: 2017-11-08
  * Time: 下午5:04
  * 参照地址：http://dongxicheng.org/framework-on-yarn/apache-spark-join-two-tables/
  */
object BroadcastJoin {
    def main(args: Array[String]): Unit = {
        val sparkConf=new SparkConf().setMaster("local[2]").setAppName("BroadcastJoin")
        val sparkContext=new SparkContext(sparkConf)
        broadcastJoin(sparkContext)
        sparkContext.stop()
    }
    def broadcastJoin(sc:SparkContext): Unit ={
      val peopleSchoolInfo = sc.parallelize(Array(("110", "ustc","211"), ("220", "xxxx","001")))
      val peopleInfo=sc.parallelize(Array(("110", "huhuniao"), ("222", "loser"))).collectAsMap
      val broadcastVar=sc.broadcast(peopleInfo)
      //小文件
      val peopleInfoMap=broadcastVar.value
      val res=peopleSchoolInfo.mapPartitions(x=>
        //for循环中的 yield 会把当前的元素记下来，保存在集合中，循环结束后将返回该集合
        for{
          (s1,s2,s3) <- x
          if(peopleInfoMap.contains(s1))
        } yield (s1, s2, peopleInfoMap.get(s1).getOrElse(""))
      )
      res.foreach(println)
    }
}
