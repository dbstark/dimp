package com.hiwan.dimp.tool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;

public class OracleDateTimeTypeTest {
	
	 private static SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd") ;
	  public static String readString(int colNum, ResultSet r) throws SQLException {
		  
		  String value ;
//		  int type = r.getMetaData().getColumnType(colNum);
		  String type = r.getMetaData().getColumnTypeName(colNum) ;
		  if("DATE".equals(type)){
			  value = sdf.format(r.getDate(colNum)) ;
		  }else{
			  value = r.getString(colNum);
		  }
		  if(value == null){
			  return "" ;
		  }else{
			  return value.replace("\r", "").trim() ;
		  }
//		  return r.getString(colNum);
	  }

	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub

		ResultSet rs = null;
		Statement stmt = null;
		Connection conn = null;
		
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conn = DriverManager.getConnection(
					"jdbc:oracle:thin:@200.144.2.164:1521:orcl", "cpdds_pdata", "pdata");
			stmt = conn.createStatement();
//			rs = stmt.executeQuery("select * from t03_apply ");
			rs = stmt.executeQuery("select * from date_time_test ");
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd") ;
			String value ;
			if(rs.next()){
				
				System.out.println(readString(1,rs));
				System.out.println(readString(2,rs));
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			if(rs != null){
				rs.close() ;
			}
			if(stmt != null){
				stmt.close() ;
			}
			if(conn != null){
				conn.close() ;
			}
		}
//		System.out.println("a");
		
	}

}
