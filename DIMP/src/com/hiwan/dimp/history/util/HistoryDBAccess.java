package com.hiwan.dimp.history.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSourceFactory;

import com.hiwan.dimp.db.DBAccess;

public class HistoryDBAccess {

	/*
	 * 用于连接oracle历史数据库
	 * */
	private static DataSource his_oracle;
	static {
		Properties prop = new Properties();
		try {
			prop.load(DBAccess.class.getClassLoader().getResourceAsStream(
					"history_oracle_config.properties"));
			his_oracle = BasicDataSourceFactory.createDataSource(prop);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取oracle数据库连接
	 * 
	 * @return
	 */
	public static Connection getConnection_ds_oracle() {
		Connection conn = null;
		try {
			conn = his_oracle.getConnection();
//			conn.createStatement().execute("alter session set current_schema=CPDDS_PDATA");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

	/**
	 * 关闭数据库连接
	 * 
	 * @param conn
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
	
	public static void main(String[] args) throws Exception {
		Connection conn = HistoryDBAccess.getConnection_ds_oracle() ;
		PreparedStatement psmt = conn.prepareStatement(" select * from CARD_INTO_INFO_FAILED where 0=1 ");
		ResultSet rs = psmt.executeQuery();
	}
	
}
