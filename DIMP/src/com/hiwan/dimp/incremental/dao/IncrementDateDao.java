package com.hiwan.dimp.incremental.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;

import com.hiwan.dimp.dao.PageToolMysql;
import com.hiwan.dimp.db.DBAccess;
import com.hiwan.dimp.incremental.bean.IncrementDate;


public class IncrementDateDao {
	
	Connection conn = null;
	public IncrementDateDao(){
		conn = DBAccess.getConnection_ds_mysql(); 
	}
	
	public List<IncrementDate> getIncreDateList( List<Map<String, Object>> list ) throws Exception {
		List<IncrementDate> incre_date_list = new ArrayList<IncrementDate>() ;
		for(Map<String, Object> map : list){
			IncrementDate incre_date = new IncrementDate() ;
			BeanUtils.populate(incre_date, map) ;
			incre_date_list.add(incre_date) ;
		}
		return incre_date_list ;
	}
	
	/**
	 * 获取加载数据的日期
	 * 拼装sql
	 * @throws Exception 
	 * */
	public Map<String, IncrementDate> getIncrementDate() throws Exception{
		Map<String, IncrementDate> map = new HashMap<String, IncrementDate>() ;
		String sql = "select * from tb_table_increment_date" ;
		List<Map<String, Object>> increment_date_map_list = PageToolMysql.allRowList(sql) ;
		List<IncrementDate> incre_date_list = this.getIncreDateList(increment_date_map_list) ;
		for(IncrementDate id : incre_date_list){
			map.put(id.getTable_name(), id) ;
		}
		return map ;
	}
	
	
	/**
	 * 获取加载数据的日期
	 * 拼装sql
	 * @throws Exception 
	 * */
	public Map<String, IncrementDate> getIncrementJobDate() throws Exception{
		Map<String, IncrementDate> map = new HashMap<String, IncrementDate>() ;
		String sql = "select * from tb_table_increment_date" ;
		List<Map<String, Object>> increment_date_map_list = PageToolMysql.allRowList(sql) ;
		List<IncrementDate> incre_date_list = this.getIncreDateList(increment_date_map_list) ;
		for(IncrementDate id : incre_date_list){
			map.put(id.getCim_job_name(), id) ;
		}
		return map ;
	}
	
	
	public void update_increment_date(IncrementDate id) {
//		Connection conn = DBAccess.getConnection_ds_mysql();
		PreparedStatement psmt = null;
		try {
			psmt = conn.prepareStatement("update tb_table_increment_date set file_date=?,load_date=? where table_name=? and cim_job_name = ?");
			psmt.setString(1, id.getFile_date()) ;
			psmt.setTimestamp(2, new Timestamp(System.currentTimeMillis())) ;
			psmt.setString(3, id.getTable_name()) ;
			psmt.setString(4, id.getCim_job_name()) ;
			psmt.executeUpdate() ;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(psmt != null){
			try {
				psmt.close() ;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void insert_increment_date(IncrementDate id) {
//		Connection conn = DBAccess.getConnection_ds_mysql();
		PreparedStatement psmt = null;
		try {
			psmt = conn.prepareStatement("insert into tb_table_increment_date(table_name,cim_job_name,load_date,file_date) values(?,?,?,?)");
			psmt.setString(1, id.getTable_name()) ;
			psmt.setString(2, id.getCim_job_name()) ;
			psmt.setTimestamp(3, new Timestamp(System.currentTimeMillis())) ;
			psmt.setString(4, id.getFile_date()) ;
			psmt.executeUpdate() ;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(psmt != null){
			try {
				psmt.close() ;
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void close_conn(){
		if(conn != null){
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
	}
	
}
