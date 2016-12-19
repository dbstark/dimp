package com.hiwan.dimp.databak.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.hiwan.dimp.databak.bean.TableBakSql;
import com.hiwan.dimp.databak.util.HiveConnection;
import com.hiwan.dimp.db.DBAccess;

public class MetaDataInfoDao {
	
	HiveConnection hive_conn = null ;
	public MetaDataInfoDao(){
		hive_conn = new HiveConnection() ;
	}

	public Map<String, TableBakSql> getTableSql(List<String> list){
		Map<String, TableBakSql> map = new HashMap<String, TableBakSql>() ;
		StringBuilder sql = new StringBuilder(" ") ;
		sql.append(" select ") ;
		sql.append(" table_name , ") ;
		sql.append(" create_script_hive , ") ;
		sql.append(" is_partition ") ;
		sql.append(" from ") ;
		sql.append(" tb_table_metadata ") ;
		sql.append(" where 1 = 1 ") ;
		if(list.size() > 0){
			sql.append(" and ( table_name = '").append(list.get(0)) ;
			for(int i = 1 ; i < list.size() ; i++){
				sql.append("' or table_name = '").append(list.get(i)) ;
			}
			sql.append("' ) ") ;
		}
		sql.append(" ") ;
		Connection conn = DBAccess.getConnection_ds_mysql();
		PreparedStatement psmt = null;
		ResultSet rs = null;
		try {
			System.out.println(sql.toString());
			psmt = conn.prepareStatement(sql.toString());
			rs = psmt.executeQuery();
			while(rs.next()){
				map.put(rs.getString(1), new TableBakSql(rs.getString(1), rs.getString(2), rs.getString(3))) ;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return map ;
	}
	
	//对Map进行处理  创建对应的数据存储表
	public void bak_sql_carry(Map<String, TableBakSql> map){
		
		String table_name = null ;
		TableBakSql tbs = null ;
		String is_partition = null ;
		String table_sql = null ;
		String create_sql = null ;
		for(Entry<String, TableBakSql> entry : map.entrySet()){
			table_name = entry.getKey() ;
			tbs = entry.getValue() ;
			table_sql = tbs.getTable_sql() ;
			is_partition = tbs.getIs_partition() ;
			create_sql = sql_change(table_sql, table_name) ;
			hive_conn.execute3(" drop table if exists " + table_name + "_hisdaybak") ;
			//执行建表
			System.out.println(create_sql) ;
			hive_conn.execute3(create_sql) ;
		}
	}
	
	public void cloes_hive(){
		hive_conn.close(); 
	}
	
	//对建表语句进行处理,并且对表名进行转换
	public String sql_change(String table_sql , String table_name){
		StringBuilder sql = new StringBuilder(" ") ;
		sql.append(" create table if not exists " ) ;
		sql.append(table_name + "_hisdaybak" ) ;
		sql.append(table_sql.substring(table_sql.indexOf("("), table_sql.indexOf(")") + 1)).append(" ") ;
		sql.append(" partitioned by (p_date string) ") ;
		sql.append(" ").append(" row format delimited fields terminated by '!' stored as textfile ").append(" ") ;
		return sql.toString() ;
	}
	
	public static void main(String[] args) {
		MetaDataInfoDao mdid = new MetaDataInfoDao() ;
		List<String> list = new ArrayList<String>() ;
		if(args == null || args.length == 0){
			
		}else{
			for(String s : args[0].split(",")){
				list.add(s.trim()) ;
			}
		}
		Map<String, TableBakSql> map = mdid.getTableSql(list) ;
		mdid.bak_sql_carry(map) ;
		mdid.cloes_hive(); 
	}
	
}
