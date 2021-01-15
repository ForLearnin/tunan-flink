package com.tunan.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

public class PhoenixUtils {
 
    private static final Logger logger  = LoggerFactory.getLogger(PhoenixUtils.class);
    private static Properties prop;
 
    static{
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        } catch (ClassNotFoundException e) {
            logger.error("PhoenixDriver 驱动加载异常");
        }

        prop = new Properties();
        prop.setProperty("hbase.rpc.timeout", "600000");
        prop.setProperty("hbase.client.scanner.timeout.period", "600000");
        prop.setProperty("dfs.client.socket-timeout", "600000");
        prop.setProperty("phoenix.query.keepAliveMs", "600000");
        prop.setProperty("phoenix.query.timeoutMs", "3600000");

    }

    static String url = "jdbc:phoenix:emr-header-1,emr-header-2,emr-worker-1:2181";
 
    private PhoenixUtils(){}
 
    /**
     * 获得phoenix SQL连接
     * @return
     * @throws SQLException
     */
    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url,prop);
    }

    /**
     * 关闭资源
     * @param con 连接
     * @param stmt 执行
     * @param rs 结果集 可填 null
     */
    public static void closeResource(Connection con, Statement stmt, ResultSet rs){
        // 先释放结果集
        if(rs != null){
            try {
                rs.close();
            } catch (SQLException e) {
                logger.error(e.getMessage(),e);
            }
        }
 
        // 然后释放执行 Statement 或 PreparedStatement
        if(stmt != null){
            try {
                stmt.close();
            } catch (SQLException e) {
                logger.error(e.getMessage(),e);
            }
        }
 
        // 最后释放连接
        if(con != null){
            try {
                con.close();
            } catch (SQLException e) {
                logger.error(e.getMessage(),e);
            }
        }
    }

    public static void main(String[] args) throws SQLException {

        Connection conn = getConnection();
        System.out.println("已经连接上Phoenix: "+conn);
    }
 
}