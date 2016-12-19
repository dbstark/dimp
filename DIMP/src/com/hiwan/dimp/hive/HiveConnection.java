package com.hiwan.dimp.hive;

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
					.getResourceAsStream("hive_config.properties"));
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
//		    stmt.execute("set hive.exec.orc.dictionary.key.size.threshold=1.0");
		    stmt.execute("set ngmr.safety.size.single.entry=-1");
		    stmt.execute("set transaction.type=inceptor");
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

	public ResultSet execute(String sql) {
//		System.out.println("--------------start to excute hive script-------: ");
//		System.out.println(sql);
		ResultSet res = null;
		try {
			res = stmt.executeQuery(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			 //close();
		}

		return res;
	}
	
	public ResultSet executeInsert(String sql) throws SQLException {
//		System.out.println("--------------start to excute hive script-------: ");
//		System.out.println(sql);
		ResultSet res = null;	
		res = stmt.executeQuery(sql);
		return res;
	}
	
	public static void main(String[] args) throws Exception {
		
		String hiveSql="create table BB_LOAN_FINANCE(HH string,APPOPKIND string,TYPETIME string,TIMEDATA string,XDCODE string,OVERAMT double,OVERINT double,CSLAMT double,CSLINT double,RETAMT double,OVERINTBAL double,CSLBAL double,OVERBAL double,LOANBAL double,CSLINTBAL double,ROW_NUM double) row format delimited fields terminated by ',' stored as textfile;";
		
		
		ResultSet rs=new HiveConnection().execute(hiveSql);
		int rowCount=0;
		System.out.println(rs.next());
		while(rs.next()){
	        	//rowCount = rowCount + 1;
			System.out.println("test");
			rowCount=rs.getInt("good");
	    }
		System.out.println(rowCount);
	}
}
