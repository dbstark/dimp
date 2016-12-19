package com.hiwan.dimp.incremental.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import com.hiwan.dimp.db.DBAccess;
import com.hiwan.dimp.incremental.bean.IncrDetailLog;

public class IncrDetailLogDao {

	Connection conn ;
	Statement stat ;
	
	public IncrDetailLogDao(){
		try {
			conn = DBAccess.getConnection_ds_mysql(); 
			stat = conn.createStatement() ;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void insert_incr_detail_log(IncrDetailLog log) throws Exception{
		
		PreparedStatement psmt = null;
		String sql = "insert into incr_detail_log(" +
				"table_name,job_name,job_type,file_date,file_list,file_num ," +
				"convertstatus,sdatastatus,pdatastatus,begintime,endtime,mark) " +
				"values(?,?,?,?,?,?,?,?,?,?,?,?)";
		
		psmt = conn.prepareStatement(sql);
		psmt.setString(1, log.getTable_name()) ;
		psmt.setString(2, log.getJob_name()) ;
		psmt.setString(3, log.getJob_type()) ;
		psmt.setString(4, log.getFile_date()) ;
		psmt.setString(5, log.getFile_list()) ;
		psmt.setInt(6, log.getFile_num()) ;
		psmt.setString(7, log.getConvertstatus()) ;
		psmt.setString(8, log.getSdatastatus()) ;
		psmt.setString(9, log.getPdatastatus()) ;
		psmt.setString(10, log.getBegintime()) ;
		psmt.setString(11, log.getEndtime()) ;
		psmt.setString(12, log.getMark()) ;
		psmt.executeUpdate() ;
	
		if (psmt != null) {
			psmt.close();
		}
		
	}
	
	public void close(){
		try {
			if(stat != null){
				stat.close() ;
			}
			if(conn != null){
				conn.close() ;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
}
