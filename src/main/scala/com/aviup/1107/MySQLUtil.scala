package com.aviup.1108

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * Created with IntelliJ IDEA.
  * Description: MySQL工具类
  * Author: lihongjie
  * Date: 2017-11-08
  * Time: 下午7:04
  */
object MySQLUtil {
  //获取连接
  def getConn() ={
    DriverManager.getConnection("jdbc:mysql://localhost:3306/lhj?user=root&password=root")
  }

  def release(conn:Connection,pstmt:PreparedStatement): Unit ={
    try{
      if (pstmt!=null){
        pstmt.close()
      }
    }catch {
      case e:Exception=>e.printStackTrace()
    }finally {
      conn.close()
    }
  }
}
