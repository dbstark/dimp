package com.hiwan.dimp.incremental.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class HiveConnection2 {
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
			prop.load(HiveConnection2.class.getClassLoader()
					.getResourceAsStream("hive_config2.properties"));
			driverClassName = prop.getProperty("driverClassName");
			url = prop.getProperty("url");
			username = prop.getProperty("username");
			password = prop.getProperty("password");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public HiveConnection2() {
		try {
//			System.out.println("创建hive链接:" + new Date());
			Class.forName(driverClassName);
			con = DriverManager.getConnection(url, username, password);
			stmt = con.createStatement();
//			System.out.println("创建hive链接end:" + new Date());
			stmt.execute("set role admin");
			stmt.execute("add jar /home/myudf.jar");
		    stmt.execute("create temporary function hash_partition AS 'com.hiwan.dimp.incremental.myudf.HashPartition'");
		    stmt.execute("create temporary function month_partition AS 'com.hiwan.dimp.incremental.myudf.MonthPartition'");
		    stmt.execute("create temporary function halfyear_partition AS 'com.hiwan.dimp.incremental.myudf.HalfYearPartition'");
		    stmt.execute("create temporary function nummonth_partition AS 'com.hiwan.dimp.incremental.myudf.NumMonthPartition'");
		    stmt.execute("create temporary function month_partition_spe AS 'com.hiwan.dimp.incremental.myudf.MonthPartitionSpe'");
		    stmt.execute("create temporary function month_del_partition AS 'com.hiwan.dimp.incremental.myudf.MonthDelPartition'");
		    stmt.execute("set ngmr.safety.size.single.entry=-1");
		    stmt.execute("set transaction.type=inceptor");
//		    stmt.execute("set hive.exec.orc.dictionary.key.size.threshold=1.0");
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

	public boolean execute(String sql) {
		boolean where = true ;
//		System.out.println("sql:" + sql);
		try {
			stmt.executeQuery(sql);
		} catch (SQLException e) {
			where = false ;
		}

		return where;
	}

	public ResultSet execute2(String sql) throws SQLException {
		ResultSet res = null;
//		System.out.println("sql:" + sql);
		res = stmt.executeQuery(sql);
		return res ;
	}
	
	public String table_hdfs_path(String table_name) throws SQLException{
		String hdfs_path = "" ;
		ResultSet rs  = null;
		rs = stmt.executeQuery(" desc formatted " + table_name) ;
		while (rs.next()) {
			if(rs.getString(1).toLowerCase().trim().equals("location:")){
				hdfs_path = rs.getString(2) ;
				break ;
			}
		}
		rs.close() ;
		return hdfs_path ;
	}
	
	public void execute3(String sql) throws SQLException {
		System.out.println("sql:" + sql);
		stmt.execute(sql) ;
	}
	
}
