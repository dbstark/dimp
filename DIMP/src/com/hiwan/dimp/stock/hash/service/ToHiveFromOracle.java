package com.hiwan.dimp.stock.hash.service;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.hiwan.dimp.stock.hash.util.HashDBAccess;
import com.hiwan.dimp.stock.hash.util.HiveConnection;

public class ToHiveFromOracle {
	
	public static void main(String[] args) throws Exception {
		ToHiveFromOracle thfo = new ToHiveFromOracle() ;
		try {
			if(args != null && args.length > 0){
				thfo.get_table_from_oracle(args[0]) ;  //输入参数为表名
			}else{
				System.out.println("请输入参数：参数为文件名(文件内容为表名	对应的分区方法)");
				System.exit(1) ;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	Map<String, String> name_par_map ;
	Map<String, String> column_type ;
	List<String> partition_list ;
	Connection oracle_conn ;
	Connection mysql_conn ;
	PreparedStatement oracle_psmt ;
	PreparedStatement mysql_psmt ;
	ResultSet rs ;
	HiveConnection hive_conn ;
//	SqoopShellExecute sse ;
	
	/**
	 * 获取hash分区表的所有需要的表
	 * */
	public void get_table_from_oracle(String arg) throws Exception{
		ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(4);
		ToHiveImportThread thit = null ;
		
//		name_par_map = new HashMap<String, String>() ;
//		sse = new SqoopShellExecute() ;
		name_par_map = table_name_part(arg) ;
		if(name_par_map.size() > 0){
			hive_conn = new HiveConnection() ;
			oracle_conn = HashDBAccess.getConnection_ds_oracle() ;
			mysql_conn = HashDBAccess.getConnection_ds_mysql() ;
		}
		System.out.println("name_par_map:" + name_par_map.size());
		for(Entry<String, String> entry : name_par_map.entrySet()){
			String table_name = entry.getKey() ;
			System.out.println(table_name);
			String part_fun = entry.getValue() ;
			column_type = new LinkedHashMap<String, String>() ;
			oracle_psmt = oracle_conn.prepareStatement(allRowListSql("CPDDS_PDATA" + "." + table_name)) ;
			rs = oracle_psmt.executeQuery() ;
			ResultSetMetaData rsmd = rs.getMetaData();// 列名元数据信息
			int count = rsmd.getColumnCount();// 字段数量
			for (int i = 1; i < count; i++) {
				String type = typeTransform(rsmd.getColumnTypeName(i));
				String name = rsmd.getColumnName(i);
				column_type.put(name, type);
			}
			rs.close(); 
			// 列名 类型
			String columnType0 = "";
			for (String key : column_type.keySet()) {
				columnType0 = columnType0 + (key + " " + column_type.get(key) + ",");
			}
			String columnType = columnType0.substring(0, columnType0.length() - 1);
			System.out.println("columnType" + columnType);
			//判断表是否为分区表select table_name,partition_name from user_tab_partitions where table_name='T01';
			oracle_psmt = oracle_conn.prepareStatement(
					" select table_name , partition_name  from dba_tab_partitions where table_name = '" + table_name + "' ");
			rs = oracle_psmt.executeQuery();
			partition_list = new LinkedList<String>() ;
			while(rs.next()){
				partition_list.add(rs.getString(2)) ;
			}
			rs.close(); 
			//生成建表语句并且进行执行
			String to_hive_name = "NORMAL_" + table_name ;
			String create_table_sql ;
			String create_orc_sql = "" ;
//			String partition_sql ;
			Map<String, String> args_map = new HashMap<String, String>() ;
			args_map.put("oracle_name", table_name) ; //oracle表名
			args_map.put("hive_name", to_hive_name) ; //hive表名
			args_map.put("partition_name", "p_date") ; //分区字段
			args_map.put("view", "CPDDS_PDATA") ; //视图
			args_map.put("part_fun", part_fun) ;
			if (partition_list.size() > 0) {
				hive_conn.execute(" drop table if exists " + to_hive_name ) ;
				hive_conn.execute(" drop table if exists " + table_name ) ;
				create_table_sql = "create table " + to_hive_name + "(" + columnType
						+ ")  row format delimited fields terminated by '!' stored as textfile;";
				//执行创建文本表语句
				hive_conn.execute(create_table_sql) ;
				//执行创建orc表语句 //根据mysql中的情况进行查询,获取oracle表的类型为信息表还是其他
				mysql_psmt = mysql_conn.prepareStatement("select create_script_hive from tb_table_metadata where table_name = '" + table_name + "' " ) ;
				rs = mysql_psmt.executeQuery() ;
				while(rs.next()){
					create_orc_sql = rs.getString(1) ;
				}
				hive_conn.execute(create_orc_sql) ;
				
				//进行存量导入,导入完成后创建orc表,然后进行分区导入
				Map<String, String> args_map_clone = new HashMap<String, String>(args_map) ;
				thit = new ToHiveImportThread(args_map_clone, partition_list, hive_conn) ;
				pool.submit(thit) ;
			}
		}
		pool.shutdown(); 
		while(true){
			if(pool.getActiveCount() == 0){
				hive_conn.close(); 
				mysql_conn.close(); 
				oracle_conn.close(); 
				break ;
			}else{
				System.out.println("thread:" + pool.getActiveCount());
				Thread.sleep(1*60*1000);
			}	
		}
	}
	
	/**
	 * 根据文件名 获取表名及对应分区方法的列表
	 * @throws IOException 
	 * */
	public static Map<String, String> table_name_part(String input_file) throws IOException{
		Map<String, String> map = new HashMap<String, String>() ;
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(input_file))) ;
		String line = "" ;
		String[] name_part_arr ;
		while((line = br.readLine()) != null){
			name_part_arr = line.split(" ") ;
			map.put(name_part_arr[0], name_part_arr[1]) ;
		}
		br.close(); 
		return map ;
	}
	
	/**
	 * 获取列名
	 * */
	public static String allRowListSql(String sql) {
		StringBuilder sb = new StringBuilder("   ");
		sb.append(" select a.*, rownum row_num ");
		sb.append(" from ( ").append(sql).append(" ) a  where 0=1");
		System.out.println(sb.toString());
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
//			hiveType = "double" ;
			hiveType = "String" ;
		}
		// 对oracle日期类型现处理为String
		if (type.equalsIgnoreCase("DATE") || type.equalsIgnoreCase("TIMESTAMP")) {
			hiveType = "string";
		}
		return hiveType;
	}
	
	
	class ToHiveImportThread implements Runnable , Serializable{

		/**
		 * serialVersionUID
		 */
		private static final long serialVersionUID = -2093054483628595934L;
		
		Map<String, String> map ;
		List<String> partition_list ;
		HiveConnection hive_conn ;
		
		public ToHiveImportThread() {
			super();
		}

		public ToHiveImportThread(Map<String, String> map,
				List<String> partition_list , HiveConnection hive_conn ) {
			super();
			this.map = map;
			this.partition_list = partition_list;
			this.hive_conn = hive_conn ;
		}
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			SqoopShellExecute sse = new SqoopShellExecute() ;
			sse.partition_table_sqoop(map) ;
			String table_name = map.get("oracle_name") ;
			String to_hive_name = map.get("hive_name") ;
			String part_fun = map.get("part_fun") ;
			String partition_sql = "" ;
			HiveConnection hive_con = new HiveConnection() ;
			for(String partition_value : partition_list){
				hive_con.execute(" alter table " + table_name + " add partition (p_date='" + partition_value + "') ") ;
				//把数据由文本表导入orc表
				partition_sql = "insert into " + table_name
						+ " partition (p_date = '" + partition_value
						+ "') select * from " + to_hive_name
						+ " where " + part_fun + "  ='" + partition_value
						+ "' ";
				System.out.println(partition_sql);
				hive_con.execute(partition_sql) ;
			}
			hive_con.execute(" drop table if exists " + to_hive_name) ;
			hive_con.close(); 
		}
		
	}
	
}
