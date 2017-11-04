package com.aviup

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * Author: lihongjie
  * Date: 2017-11-04
  * Time: 下午2:36
  */
object ScalaTest {
    def main(args: Array[String]): Unit = {
        val conf=new Configuration()
        conf.set("fs.default.name","hdfs://192.168.31.89:8020")
        val outputPath="hdfs://192.168.31.89:8020/spark/emp/"
        val loadTime="201711112025"
        val partition="/d=20171111/h=20"
        val fileSystem=FileSystem.get(URI.create(outputPath + "temp/" + loadTime + partition),conf)
        changeFileName(fileSystem,outputPath,loadTime,partition)
    }

  def changeFileName(fileSystem: FileSystem, outputPath: String, loadTime: String, partition: String): Unit = {
    val paths = SparkHadoopUtil.get.globPath(new Path(outputPath + "temp/" + loadTime + partition + "/*.txt"))
    var times = 0
    paths.map(x => {
      var newLocation = x.toString.replace(outputPath + "temp/" + loadTime, outputPath + "data/")
      println("1:"+newLocation)
      newLocation = newLocation.replace("part-r-", "")
      val index = newLocation.lastIndexOf("/")
      times += 1
      newLocation = newLocation.substring(0, index + 1) + loadTime + "-" + times + ".txt"
      println("2:"+newLocation)
      val officialPath = new Path(newLocation)
      if (!fileSystem.exists(officialPath.getParent))
        fileSystem.mkdirs(officialPath.getParent)
      fileSystem.rename(x, officialPath)
    })
  }
}
