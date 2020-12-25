package com.tunan.utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}


object MySQLUtils {

  def getConnection: Connection = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://aliyun:3306/study?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=GMT%2B8","root","juan970907!@#")
  }

  def close(connection:Connection, pstmt:PreparedStatement,rs:ResultSet): Unit = {
    if(null != pstmt) pstmt.close()
    if(null != connection) connection.close()
    if(null != rs) rs.close()
  }
}
