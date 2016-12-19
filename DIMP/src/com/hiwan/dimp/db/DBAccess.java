package com.hiwan.dimp.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSourceFactory;

public class DBAccess {
	private static DataSource ds_oracle;
	private static DataSource ds_mysql;
	 
	
	static{
		Properties prop = new Properties();
		try {
			prop.load(DBAccess.class.getClassLoader().getResourceAsStream("oracle_config.properties"));			 				
			ds_oracle = BasicDataSourceFactory.createDataSource(prop);
			 
			prop.load(DBAccess.class.getClassLoader().getResourceAsStream("mysql_config.properties"));
			ds_mysql = BasicDataSourceFactory.createDataSource(prop);
			 							
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	

	/**
	 * 获取oracle数据库连接
	 * @return
	 */
	public static Connection getConnection_ds_oracle(){
		Connection conn = null;	
		try {
			conn = ds_oracle.getConnection();
			conn.createStatement().execute("alter session set current_schema=CPDDS_PDATA");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}
	
	/**
	 * 获取mysql数据库连接
	 * @return
	 */
	public static Connection getConnection_ds_mysql(){
		Connection conn = null;
		 
		try {
			conn = ds_mysql.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}
	
	/**
	 * 关闭数据库连接
	 * @param conn
	 */
	public static void closeConnection(Connection conn){
		if(conn!=null){
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	public static void main(String[] args) throws SQLException {
		System.out.println(DBAccess.getConnection_ds_oracle());
		System.out.println(DBAccess.getConnection_ds_mysql());
	}
}
