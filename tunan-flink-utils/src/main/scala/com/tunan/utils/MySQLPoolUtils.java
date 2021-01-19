package com.tunan.utils;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MySQLPoolUtils {

    private static DruidDataSource ds;

    public static String RUL = "jdbc:mysql://aliyun:3306/study?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=GMT%2B8";
    public static String USER = "root";
    public static String PASSWORD = "juan970907!@#";
    public static String DRIVER = "com.mysql.jdbc.Driver";

    static {
        try {

            // 创建DataSource 连接池
            ds = new DruidDataSource();
            ds.setDriverClassName(DRIVER);
            ds.setUsername(USER);
            ds.setPassword(PASSWORD);
            ds.setUrl(RUL);
            ds.setInitialSize(5);
            ds.setMinIdle(10);
            ds.setMaxActive(20);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static DruidDataSource getDataSource() throws SQLException {
        return ds;
    }


    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }

    public static void close(ResultSet result, Statement state, Connection conn) {
        if (null != result) {
            try {
                result.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (null != state) {
            try {
                state.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
