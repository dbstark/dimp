package com.hiwan.dimp.incremental.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.hiwan.dimp.db.DBAccess;
import com.hiwan.dimp.incremental.util.HiveConnection;

public class JobToTableDao {

	public String job_to_table_sql(){
		StringBuilder sql = new StringBuilder(" ") ;
		sql.append(" select interface_code , cim_job_name ") ;
		sql.append(" from tb_table_file_con ") ;
		return sql.toString() ;
	}
	
	public Map<String, String> job_to_table_map(){
		Map<String, String> map = new HashMap<String, String>() ;
		Connection conn = DBAccess.getConnection_ds_mysql();
		PreparedStatement psmt = null ;
		ResultSet rs = null ;
		try {
			psmt = conn.prepareStatement(job_to_table_sql()) ;
			rs = psmt.executeQuery() ;
			while(rs.next()){
				map.put(rs.getString(2), rs.getString(1)) ;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return map ;
	}
	
	public Map<String, String> job_table_map_from_hive() throws SQLException{
		Map<String, String> map = new HashMap<String, String>() ;
		HiveConnection hive_conn = new HiveConnection() ;
		String sql = "select interface_code , cim_job_name from mpm.int_info " ;
		ResultSet rs = null ;
		rs = hive_conn.execute2(sql) ;
		while(rs.next()){
			map.put(rs.getString(2).trim().toUpperCase(), rs.getString(1).trim().toUpperCase()) ;
		}
		hive_conn.close() ;
		return map ;
	}
	
}
