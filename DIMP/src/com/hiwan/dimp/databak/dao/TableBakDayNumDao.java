package com.hiwan.dimp.databak.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.hiwan.dimp.db.DBAccess;

public class TableBakDayNumDao {

	public Map<String, Integer> table_day() throws SQLException{
		Map<String, Integer> map = new HashMap<String, Integer>() ;
		StringBuilder sql = new StringBuilder(" ") ;
		sql.append(" select * from inc_save_dur ") ;
		Connection conn = DBAccess.getConnection_ds_mysql();
		PreparedStatement psmt = null;
		ResultSet rs = null;
		psmt = conn.prepareStatement(sql.toString());
		rs = psmt.executeQuery();
		while(rs.next()){
			map.put(rs.getString(1), rs.getInt(2)) ;
		}
		
		return map ;
	}
	
	
}
