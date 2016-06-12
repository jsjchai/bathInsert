package model;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class DB {

    public static Connection getConnection() {
        Connection conn = null;
        Properties pro = new Properties();
        try {
            pro.load(DB.class.getResourceAsStream("jdbc.properties"));
            Class.forName(pro.getProperty("jdbc.driverClassName")).newInstance();
            conn = DriverManager.getConnection(pro.getProperty("jdbc.url"), pro.getProperty("jdbc.username"),
                    pro.getProperty("jdbc.password"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static Statement getStatement(Connection conn) {
        Statement stat = null;
        try {
            stat = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return stat;
    }

    public static void executeUpdate(Statement stat, String sql) {
        try {
            stat.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    public static PreparedStatement  getPreparedStatement(Connection conn,String sql){
        PreparedStatement pre=null;
        try {
            pre = conn.prepareStatement(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return pre;
    }
    
    public static void close(Connection conn){
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if(conn!=null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    public static void close(Statement stat){
        try {
            stat.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if(stat!=null){
                try {
                    stat.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
