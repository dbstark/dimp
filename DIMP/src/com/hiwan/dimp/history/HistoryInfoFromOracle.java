package com.hiwan.dimp.history;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.hiwan.dimp.history.bean.HistoryTableInfo;
import com.hiwan.dimp.history.master.SqoopShellExecute;
import com.hiwan.dimp.history.util.HistoryDBAccess;
import com.hiwan.dimp.history.util.HiveConnection;

public class HistoryInfoFromOracle {

	
	public static void main(String[] args) {
		HistoryInfoFromOracle hifo = new HistoryInfoFromOracle() ;
		try {
			if(args != null && args.length > 0){
				System.out.println("有参数:导入指定的某些表,表名以,分隔");
				hifo.get_table_from_oracle(args[0]) ;
			}else{
				System.out.println("没有参数:导入全部的表");
				hifo.get_table_from_oracle("") ;
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取历史数据数据库信息
	 * */
	/**
	 * 历史数据库表名列表
	 * 
	 * */
	List<String> table_name_list ;
	Map<String, HistoryTableInfo> table_map ;
	Map<String, String> column_type ;
	List<String> partition_list ;
	Connection conn ;
	PreparedStatement psmt ;
	ResultSet rs ;
	HiveConnection hive_conn ;
	SqoopShellExecute sse ;
	/**
	 * 获取历史数据库下的所有需要的表
	 * @throws SQLException 
	 * */
	public void get_table_from_oracle(String arg) throws SQLException{
		sse = new SqoopShellExecute() ;
		hive_conn = new HiveConnection() ;
		conn = HistoryDBAccess.getConnection_ds_oracle() ;
		table_name_list = new ArrayList<String>() ;
		if("".equals(arg)){
			psmt = conn.prepareStatement(" select table_name from user_tables where table_name not like '%$%' ");
			rs = psmt.executeQuery();
			while(rs.next()){
				table_name_list.add(rs.getString(1)) ;
			}
		}else{
			for(String a : arg.split(",")){
				table_name_list.add(a.trim()) ;
			}
		}
		String sql ;
		for (String table_name : table_name_list) {
			sql = allRowListSql(table_name);
			column_type = new LinkedHashMap<String, String>();
			
			psmt = conn.prepareStatement(sql);
			rs = psmt.executeQuery();

			ResultSetMetaData rsmd = rs.getMetaData();// 列名元数据信息
			int count = rsmd.getColumnCount();// 字段数量
			for (int i = 1; i < count; i++) {
				String type = typeTransform(rsmd.getColumnTypeName(i));
				String name = rsmd.getColumnName(i);
				column_type.put(name, type);
			}
			// 列名 类型
			String columnType0 = "";
			for (String key : column_type.keySet()) {
				columnType0 = columnType0 + (key + " " + column_type.get(key) + ",");
			}
			String columnType = columnType0.substring(0, columnType0.length() - 1);
			
			//判断表是否为分区表select table_name,partition_name from user_tab_partitions where table_name='T01';
			psmt = conn.prepareStatement(" select table_name , partition_name  from user_tab_partitions where table_name = '" + table_name + "' ");
			rs = psmt.executeQuery();
			partition_list = new LinkedList<String>() ;
			while(rs.next()){
				partition_list.add(rs.getString(2)) ;
			}
			//生成建表语句并且进行执行
			String to_hive_name = "HIS_" + table_name ;
			String create_table_sql ;
			Map<String, String> args_map = new HashMap<String, String>() ;
			args_map.put("oracle_name", table_name) ; //oracle表名
			args_map.put("hive_name", to_hive_name) ; //hive表名
			args_map.put("partition_name", "p_date") ; //分区字段
			args_map.put("view", "CPDDS_PDATA") ; //视图
			if (partition_list.size() > 0) {
				System.out.println("分区表");
				System.out.println(" drop table if exists " + to_hive_name );
				hive_conn.execute(" drop table if exists " + to_hive_name ) ;
				create_table_sql = "create table " + to_hive_name + "(" + columnType
						+ ") partitioned by (p_date string) row format delimited fields terminated by '!' stored as textfile;";
				//执行建表语句
				System.out.println(create_table_sql);
				hive_conn.execute(create_table_sql) ;
				//创建分区
				for(String partition_value : partition_list){
					System.out.println(" alter table " + to_hive_name + " add partition (p_date='" + partition_value + "') ");
					hive_conn.execute(" alter table " + to_hive_name + " add partition (p_date='" + partition_value + "') ") ;
					//执行sqoop导入,每个分区导入一次
					args_map.put("partition_value", partition_value) ;
					sse.partition_table_sqoop(args_map) ;
				}
				
			} else {
				System.out.println("非分区表");
				System.out.println(" drop table if exists " + to_hive_name );
				hive_conn.execute(" drop table if exists " + to_hive_name ) ;
				create_table_sql = "create table " + to_hive_name + "(" + columnType
						+ ") row format delimited fields terminated by '!' stored as textfile;";
				//执行建表语句
				System.out.println(create_table_sql);
				hive_conn.execute(create_table_sql) ;
				//执行sqoop导入
				sse.not_partition_table_sqoop(args_map) ;
				
			}
		}
	}
	
	/**
	 * 获取列名
	 * */
	public static String allRowListSql(String sql) {
		StringBuilder sb = new StringBuilder("   ");
		sb.append(" select a.*, rownum row_num ");
		sb.append(" from ( ").append(sql).append(" ) a  where 0=1");
		return sb.toString();
	}
	
	/**
	 * 用于转换数据类型
	 * oracle转换成支持hive的数据类型
	 * */
	public static String typeTransform(String type) {
		String hiveType = "string";
		if (type.equalsIgnoreCase("VARCHAR2") || type.equalsIgnoreCase("CHAR")) {
			hiveType = "string";
		}
		if (type.equalsIgnoreCase("Long") || type.equalsIgnoreCase("CLOB")
				|| type.equalsIgnoreCase("BLOB")
				|| type.equalsIgnoreCase("NCLOB")
				|| type.equalsIgnoreCase("BFILE")) {
			hiveType = "string";
		}
		if (type.equalsIgnoreCase("NUMBER")) {
			hiveType = "double";
		}
		// 对oracle日期类型现处理为String
		if (type.equalsIgnoreCase("DATE") || type.equalsIgnoreCase("TIMESTAMP")) {
			hiveType = "string";
		}
		return hiveType;
	}
	
}
