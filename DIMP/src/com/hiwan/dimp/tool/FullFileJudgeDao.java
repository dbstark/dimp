package com.hiwan.dimp.tool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.hiwan.dimp.db.DBAccess;
import com.hiwan.dimp.incremental.util.HiveConnection;

public class FullFileJudgeDao {
	
	Connection conn = null;
	public FullFileJudgeDao(){
		conn = DBAccess.getConnection_ds_mysql(); 
	}
	
	public Map<String, String> cim_job_date(){
		Map<String, String> map = new HashMap<String, String>() ;
		PreparedStatement psmt = null;
		ResultSet rs = null ;
		try {
			psmt = conn.prepareStatement("select cim_job_name,file_date from tb_table_increment_date ");
			rs = psmt.executeQuery();
			while(rs.next()){
				map.put(rs.getString(1).trim().toUpperCase(), rs.getString(2).trim().toUpperCase()) ;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return map ;
	}
	
	
	public List<String> table_list(){
		List<String> list = new ArrayList<String>() ;
		PreparedStatement psmt = null;
		ResultSet rs = null ;
		try {
			psmt = conn.prepareStatement("select table_id , table_name from (" +
					"select aug.table_id table_id,meta.table_name table_name from tb_table_augment aug left join tb_table_metadata meta  " +
					"on aug.table_id = meta.id ) t");
			rs = psmt.executeQuery();
			System.out.println("Connection:" + conn);
			System.out.println("PreparedStatement:" + psmt);
			System.out.println("ResultSet:" + rs);
			while(rs.next()){
				System.out.println("table_name:"+rs.getString(2));
				list.add(rs.getString(2).trim().toUpperCase()) ;
			}
		} catch (SQLException e) {
			System.out.println(list.size());
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return list ;
	}
	
	public Map<String, String> job_table_map_from_hive(){
		Map<String, String> map = new HashMap<String, String>() ;
		HiveConnection hive_conn = new HiveConnection() ;
		String sql = "select interface_code , cim_job_name from mpm.int_info " ;
		ResultSet rs = null ;
		try {
			rs = hive_conn.execute2(sql) ;
			while(rs.next()){
				map.put(rs.getString(2).trim().toUpperCase(), rs.getString(1).trim().toUpperCase()) ;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		hive_conn.close() ;
		return map ;
	}
	
	public static void main(String[] args){
		Map<String, String> map1 = new HashMap<String, String>() ;
		map1.put("1", "1") ;
		map1.put("2", "2") ;
		Map<String, String> map2 = new HashMap<String, String>(map1) ;
		map1.remove("1") ;
		for(Entry<String, String> entry : map2.entrySet()){
			System.out.println("key:" + entry.getKey() + "\tvalue:" + entry.getValue() );
		}
		System.out.println("\n");
		for(Entry<String, String> entry : map1.entrySet()){
			System.out.println("key:" + entry.getKey() + "\tvalue:" + entry.getValue() );
		}
		
	}
	
	
}
