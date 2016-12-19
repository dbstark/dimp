package com.hiwan.dimp.databak.util ;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class HiveConnection {
	private Connection con;
	private Statement stmt;
	//private DataSource ds_hive;

	static String driverClassName;
	static String url;
	static String username;
	static String password;

	static {
		Properties prop = new Properties();
		try {
			prop.load(HiveConnection.class.getClassLoader()
					.getResourceAsStream("hive_config2.properties"));
			driverClassName = prop.getProperty("driverClassName");
			url = prop.getProperty("url");
			username = prop.getProperty("username");
			password = prop.getProperty("password");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public HiveConnection() {
		try {
			Class.forName(driverClassName);
			con = DriverManager.getConnection(url, username, password);
			stmt = con.createStatement();
			stmt.execute("set role admin");
			} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void close() {
		if (stmt != null) {
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		if (con != null) {
			try {
				con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	//DML
	public ResultSet execute2(String sql) throws SQLException {
		ResultSet res = null;
		System.out.println("sql:" + sql);
		res = stmt.executeQuery(sql);
		return res ;
	}
	
	//DDL
	public void execute3(String sql) {
		try {
			System.out.println("sql:" + sql);
			stmt.execute(sql) ;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		
}
