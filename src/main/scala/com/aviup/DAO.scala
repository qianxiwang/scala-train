package com.aviup

import java.sql.{Connection, PreparedStatement}

/**
  * Created with IntelliJ IDEA.
  * Description: 
  * Author: lihongjie
  * Date: 2017-11-08
  * Time: 下午7:06
  */
object DAO {
  //将某个app处理的数据条数写入mysql
  def SaveAccumulatorToMySQL(appName:String,times:Long): Unit ={
    val table="accumulator"
    var conn:Connection= null
    var pstmt:PreparedStatement = null
    try {
      conn=MySQLUtil.getConn()
      val sql="INSERT INTO accumulator (appName, times) VALUES (?, ?)"
      pstmt=conn.prepareStatement(sql)
      pstmt.setString(1,appName)
      pstmt.setLong(2,times)
      pstmt.executeUpdate()
    }catch {
      case e:Exception=>e.printStackTrace()
    }finally {
      MySQLUtil.release(conn,pstmt)
    }
  }
}
