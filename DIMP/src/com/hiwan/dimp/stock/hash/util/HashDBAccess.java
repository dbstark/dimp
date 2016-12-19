package com.hiwan.dimp.stock.hash.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSourceFactory;

import com.hiwan.dimp.db.DBAccess;

public class HashDBAccess {

	/*
	 * 用于连接oracle,mysql数据库
	 * */
	private static DataSource hash_oracle ;
	private static DataSource hash_mysql ;
	
	static {
		Properties prop = new Properties();
		try {
			prop.load(DBAccess.class.getClassLoader().getResourceAsStream("oracle_config.properties"));
			hash_oracle = BasicDataSourceFactory.createDataSource(prop);
			
			prop.load(DBAccess.class.getClassLoader().getResourceAsStream("mysql_config.properties"));
			hash_mysql = BasicDataSourceFactory.createDataSource(prop);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取oracle数据库连接
	 */
	public static Connection getConnection_ds_oracle() {
		Connection conn = null;
		try {
			conn = hash_oracle.getConnection();
//			conn.createStatement().execute("alter session set current_schema=CPDDS_PDATA");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}
	
	/**
	 * 获取oracle数据库连接
	 */
	public static Connection getConnection_ds_mysql() {
		Connection conn = null;
		try {
			conn = hash_mysql.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}
	

	/**
	 * 关闭数据库连接
	 */
	public static void closeConnection(Connection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 关闭mysql数据库连接
	 */
	public static void closeMysqlConnection(Connection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Connection conn = HashDBAccess.getConnection_ds_oracle() ;
		PreparedStatement psmt = conn.prepareStatement(" select * from CARD_INTO_INFO_FAILED where 0=1 ");
		ResultSet rs = psmt.executeQuery();
	}
	
}
