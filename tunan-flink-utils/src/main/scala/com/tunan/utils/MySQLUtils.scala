package com.tunan.utils

import java.sql.{Connection, DriverManager, PreparedStatement}


object MySQLUtils {

  def getConnection: Connection = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://aliyun:3306/study","root","juan970907!@#")
  }

  def close(connection:Connection, pstmt:PreparedStatement): Unit = {
    if(null != pstmt) pstmt.close()
    if(null != connection) connection.close()
  }
}
