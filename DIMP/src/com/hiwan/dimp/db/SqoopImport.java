package com.hiwan.dimp.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.hiwan.dimp.bean.StockMetaInfo;
import com.hiwan.dimp.hive.HiveConnection;

public class SqoopImport {
	private static String url ;
	private static String username;
	private static String password;
	 	
	static{
		Properties prop = new Properties();
		try {
			prop.load(DBAccess.class.getClassLoader().getResourceAsStream("oracle_config.properties"));
			url= prop.getProperty("url");
			username= prop.getProperty("username");
			password= prop.getProperty("password");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 导入结果     1:正常;0:异常
	 * @param map
	 * @return
	 */
	public static int impTableData(Map<String, String> map) {
		int n=1;
		try {
			if (map.get("tableType").equals("配置表")) {				
				n=impConfigurelAllData(map);
			}
			if (map.get("tableType").equals("信息表")) {
				if (map.get("partition_name") != null) {
					// 执行分区导入
					n=impInfoDataPartition(map);
				} else {
					n=impInfoAllData(map);
				}
			}
			if (map.get("tableType").equals("汇总表")) {
				if (map.get("partition_name") != null) {
					// 执行分区导入
					n=impParData(map);
				} else {
					n=impConfigurelAllData(map);
				}
			}
			if (map.get("tableType").equals("明细表")) {
				if (map.get("partition_name") != null) {
					n=impParData(map);
				} else {
					n=impConfigurelAllData(map);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return n;
	}
	/**
	 * sqoop导入明细表和汇总表 分区数据
	 * @param map
	 */
	private static int impParData(Map<String, String> map) {
		String num0 ="1";
		if(StringUtils.isNotBlank(map.get("num"))&&!map.get("num").trim().equals("0")){
			num0=map.get("num").toString();
		}
		Integer num=Integer.valueOf(num0.trim())<=3?3:Integer.valueOf(num0.trim());
		
		int n = 1;
		try{
			StringBuilder command = new StringBuilder("");
			
			HiveConnection hc = new HiveConnection();
			String dropTable = " drop table if exists " +map.get("tableName").toString().trim().toUpperCase()+"_"+map.get("partition_value");
			hc.execute(dropTable);
			
			//sqoop import -D oraoop.block.allocation=RANDOM -D oracle.row.fetch.size=10000 -D oraoop.import.partitions='P201211'
			//--direct  --connect jdbc:oracle:thin:@21.144.56.131:1521:orcl1 --username cpdds_pdata --password cpdds_pdata__  
			//--table T98_INDPTY_PROD_STAT  
			//--create-hive-table --hive-import --hive-table T98_INDPTY_PROD_STAT 
			//--fields-terminated-by "\0001" --delete-target-dir --num-mappers 50 ;
			
			command.append("sqoop import -D oraoop.block.allocation=RANDOM -D oracle.row.fetch.size=10000 ").append("-D oraoop.import.partitions="+map.get("partition_value")+"")									
			.append(" --direct  --connect ").append(url).append(" --username ").append(username.toUpperCase()).append(" --password ").append(password)	
			.append(" --table ").append("CPDDS_PDATA."+map.get("tableName").toString().trim().toUpperCase())
			.append(" --hive-import --hive-table ").append(map.get("tableName").toString().trim().toUpperCase())
			.append(" --hive-partition-key "+map.get("partition_name")) //
			.append(" --hive-partition-value "+map.get("partition_value"))
			.append(" --target-dir /user/root/"+map.get("tableName").toString().trim().toUpperCase()+"_"+map.get("partition_value"))
			.append(" --fields-terminated-by ! --delete-target-dir --num-mappers "+num+" ");
			System.out.println(" -------- sqoop import -------- :"+command.toString());
			//linux 命令
			Process process = Runtime.getRuntime().exec(command.toString());
			//windows 
			//Process process = Runtime.getRuntime().exec(new String[]{"notepad.exe","F:\\long_cennect.txt"});
			StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "ERROR",map.get("tableName").toString().trim().toUpperCase()+"_"+map.get("partition_value"));
			errorGobbler.start(); 
			StreamGobbler outGobbler = new StreamGobbler(process.getInputStream(), "STDOUT",map.get("tableName").toString().trim().toUpperCase()+"_"+map.get("partition_value"));
			outGobbler.start(); 
			// 正常结束0 异常1
			n = process.waitFor();		 			
		} catch (Exception e) {
			e.printStackTrace();
		}
		//返回值转换处理  1 正常 0异常
		return n==0?1:0;
		
	}
	/**
	 * sqoop导入信息表全量导入,一次性全量导入
	 * @param smi
	 */
	public static int impInfoAllData(Map<String, String> map){
		String num0 ="";
		if(StringUtils.isNotBlank(map.get("num"))&&!map.get("num").trim().equals("0")){
			num0=map.get("num").toString();
		}
		Integer num=Integer.valueOf(num0.trim())<=3?3:Integer.valueOf(num0.trim());
		int n = 1;
		String insert_into = null ;
		HiveConnection hc = new HiveConnection();
		try{	
			//删除临时表
			String dropTable = " drop table if exists " +map.get("tableName").toString().trim().toUpperCase()+"_hiveTemp";
			hc.execute(dropTable);
			StringBuilder command = new StringBuilder("");
			//sqoop import -D oraoop.block.allocation=RANDOM -D oracle.row.fetch.size=10000 
			//--direct  --connect jdbc:oracle:thin:@21.144.56.131:1521:orcl1 --username cpdds_pdata --password cpdds_pdata__  
			//--table T98_INDPTY_PROD_STAT  
			//--create-hive-table --hive-import --hive-table T98_INDPTY_PROD_STAT 
			//--fields-terminated-by "\0001" --delete-target-dir --num-mappers 50 ;
			
			command.append("sqoop import -D oraoop.block.allocation=RANDOM -D oracle.row.fetch.size=10000 ")									
			.append(" --direct  --connect ").append(url).append(" --username ").append(username.toUpperCase()).append(" --password ").append(password)	
			.append(" --table ").append("CPDDS_PDATA."+map.get("tableName").toString().trim().toUpperCase())
			//orc临时表增加后缀temp
			.append(" --create-hive-table --hive-import --hive-table ").append(map.get("tableName").toString().trim().toUpperCase()+"_hiveTemp")
			.append(" --fields-terminated-by ! --delete-target-dir --num-mappers "+num+" ");
			
			System.out.println(" -------- sqoop import -------- :"+command.toString());
			//linux 命令
			Process process = Runtime.getRuntime().exec(command.toString());
			//windows 
			//Process process = Runtime.getRuntime().exec(new String[]{"notepad.exe","F:\\long_cennect.txt"});
			StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "ERROR",map.get("tableName").toString().trim().toUpperCase());
			errorGobbler.start(); 
			StreamGobbler outGobbler = new StreamGobbler(process.getInputStream(), "STDOUT",map.get("tableName").toString().trim().toUpperCase());
			outGobbler.start(); 
			// 正常结束0 异常1
			n = process.waitFor();
			if(n==0)
			{
				//将orc中间表的数据插入到Hive目标表				
				insert_into = " INSERT INTO TABLE "+map.get("tableName").toString().trim().toUpperCase()+" SELECT * FROM " + map.get("tableName").toString().trim().toUpperCase()+"_hiveTemp";
				ResultSet rs = hc.executeInsert(insert_into);
				rs.close();
				hc.close();
			}

		} catch (SQLException e) {
			System.out.println(e.toString());
			System.out.println("hive_exception:" + insert_into );
			try {
				hc.executeInsert(insert_into) ;
				hc.close();
			} catch (Exception e2) {
				System.out.println("hive_exception2:" + insert_into );
				n = 1 ;
			}	
			return n==0?1:0;
		}catch(Exception ee)
		{
			ee.printStackTrace();
			n=1;
		}
		//返回值转换处理  1 正常 0异常
		return n==0?1:0;
	}
	/**
	 * 信息表分区数据导入
	 * @param map
	 * @return
	 */
	public static int impInfoDataPartition(Map<String, String> map){
		String num0 ="1";
		if(StringUtils.isNotBlank(map.get("num"))&&!map.get("num").trim().equals("0")){
			num0=map.get("num").toString();
		}
		Integer num=Integer.valueOf(num0.trim())<=3?3:Integer.valueOf(num0.trim());
		
		int n = 1;
		String insert_into = null ;
		HiveConnection hc = new HiveConnection();
		try{			 
			StringBuilder command = new StringBuilder("");
			String dropTable = " drop table if exists " +map.get("tableName").toString().trim().toUpperCase()+"_"+map.get("partition_value");
			hc.execute(dropTable);
			//sqoop import -D oraoop.block.allocation=RANDOM -D oracle.row.fetch.size=10000 -D oraoop.import.partitions='P201211'
			//--direct  --connect jdbc:oracle:thin:@21.144.56.131:1521:orcl1 --username cpdds_pdata --password cpdds_pdata__  
			//--table T98_INDPTY_PROD_STAT  
			//--create-hive-table --hive-import --hive-table T98_INDPTY_PROD_STAT 
			//--fields-terminated-by "\0001" --delete-target-dir --num-mappers 50 ;
			
			command.append("sqoop import -D oraoop.block.allocation=RANDOM -D oracle.row.fetch.size=10000 ").append("-D oraoop.import.partitions="+map.get("partition_value")+"")									
			.append(" --direct  --connect ").append(url).append(" --username ").append(username.toUpperCase()).append(" --password ").append(password)	
			.append(" --table ").append("CPDDS_PDATA."+map.get("tableName").toString().trim().toUpperCase())
			//orc临时表增加后缀 分区值
			.append(" --create-hive-table --hive-import --hive-table ").append(map.get("tableName").toString().trim().toUpperCase()+"_"+map.get("partition_value"))
			.append(" --target-dir /user/root/"+map.get("tableName").toString().trim().toUpperCase()+"_"+map.get("partition_value"))
			.append(" --fields-terminated-by ! --delete-target-dir --num-mappers "+num+" ");
			
			System.out.println(" -------- sqoop import -------- :"+command.toString());
			//linux 命令
			Process process = Runtime.getRuntime().exec(command.toString());
			//windows 
			//Process process = Runtime.getRuntime().exec(new String[]{"notepad.exe","F:\\long_cennect.txt"});
			StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "ERROR",map.get("tableName").toString().trim().toUpperCase()+"_"+map.get("partition_value").toString());
			errorGobbler.start(); 
			StreamGobbler outGobbler = new StreamGobbler(process.getInputStream(), "STDOUT",map.get("tableName").toString().trim().toUpperCase()+"_"+map.get("partition_value"));
			outGobbler.start(); 
			// 正常结束0 异常1
			n = process.waitFor();
			//INSERT INTO TABLE t98_indpty_prod_stat partition(p_date='p201211') SELECT * FROM t98_indpty_prod_stat_p201211
			//将orc中间表的数据插入到Hive目标表				
			if(n == 0){
				insert_into = " INSERT INTO TABLE "+map.get("tableName").toString().trim().toUpperCase()+" partition(p_date='"+map.get("partition_value")+"') SELECT * FROM " + map.get("tableName").toString().trim().toUpperCase()+"_"+map.get("partition_value");
				ResultSet rs = hc.executeInsert(insert_into);
				rs.close();
				hc.close();
			}
		} catch (SQLException e) {
			System.out.println(e.toString());
			System.out.println("hive_exception:" + insert_into );
			try {
				hc.executeInsert(insert_into) ;
				hc.close();
			} catch (Exception e2) {
				System.out.println("hive_exception2:" + insert_into );
				n = 1 ;
			}	
			return n==0?1:0;
		} catch (Exception ee) {
			ee.printStackTrace();
			n=1;
		}
		//返回值转换处理  1 正常 0异常
		return n==0?1:0;
	}
	/**
	 * sqoop配置表全量导入,全量覆盖导入
	 * @param smi
	 */
	public static int impConfigurelAllData(Map<String, String> map){
		String num ="4";
		if(StringUtils.isNotBlank(map.get("num"))&&!map.get("num").trim().equals("0")){
			num=map.get("num").toString();
		}		
		int n = 1;
		try{
			//sqoop import -D oraoop.block.allocation=RANDOM -D oracle.row.fetch.size=10000 --direct --connect jdbc:oracle:thin:@192.168.4.178:1521:orcl1 --username cpdds_pdata --password cpdds_pdata__ --table T98_INDPTY_PROD_STAT  --target-dir /user/root/T98_INDPTY_PROD_STAT --hive-import --hive-table T98_INDPTY_PROD_STAT --fields-terminated-by '!' --hive-overwrite  --delete-target-dir --num-mappers 100
			StringBuilder command = new StringBuilder("");
			command.append("sqoop import -D oraoop.block.allocation=RANDOM -D oracle.row.fetch.size=10000 --direct --connect ")
			//jdbc:oracle:thin:@192.168.4.178:1521:orcl --username cpdds_pdata --password pdata 
		    .append(url).append(" --username ").append(username.toUpperCase()).append(" --password ").append(password)	
			// --table T98_INDPTY_PROD_STAT  --target-dir /user/root/T98_INDPTY_PROD_STAT 
			.append(" --table ").append("CPDDS_PDATA."+map.get("tableName").trim().toUpperCase())
			.append(" --target-dir /user/root/").append(map.get("tableName").trim().toUpperCase())
			// --hive-import --hive-table T98_INDPTY_PROD_STAT --fields-terminated-by '!'  --hive-overwrite  --delete-target-dir --num-mappers 100
			.append(" --hive-import --hive-table ").append(map.get("tableName").trim().toUpperCase())
			.append(" --fields-terminated-by ! --hive-overwrite --delete-target-dir --num-mappers ").append(num);
			System.out.println("sqoop import -------- :"+command.toString());
			//linux 命令
			Process process = Runtime.getRuntime().exec(command.toString());
			//windows 
			//Process process = Runtime.getRuntime().exec(new String[]{"notepad.exe","F:\\long_cennect.txt"});
			StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "ERROR",map.get("tableName").toString().trim().toUpperCase());
			errorGobbler.start(); 
			StreamGobbler outGobbler = new StreamGobbler(process.getInputStream(), "STDOUT",map.get("tableName").toString().trim().toUpperCase());
			outGobbler.start(); 
			// 正常结束0 异常1
			n = process.waitFor(); 
		} catch (Exception e) {
			e.printStackTrace();
		}
		//返回值转换处理  1 正常 0异常
		return n==0?1:0;
	}

	public static void main(String[] args) throws SQLException {
		StockMetaInfo smi = new StockMetaInfo();
		
	smi.setTable_name("T98_INDPTY_PROD_STAT");
		smi.setPrimary_key("CSTM_NO");
		smi.setTable_type("配置表");
		
		Map<String, String> impMap = new HashMap<String, String>();
		/*	
		impMap.put("tableName", "T98_INDPTY_PROD_STAT");
		impMap.put("primaryKey", smi.getPrimary_key());
		impMap.put("num", "4");
		impMap.put("tableType",smi.getTable_type());
		
		SqoopImport.impTableData(impMap);*/
		
		smi.setTable_name("BB_ZD_HH");
		smi.setPrimary_key("DATEDATA,HH");
		smi.setTable_type("信息表");
		
		impMap.put("tableName", "T98_INDPTY_PROD_STAT");
		impMap.put("primaryKey", smi.getPrimary_key());
		impMap.put("num", "4");
		impMap.put("tableType",smi.getTable_type());
		
		 
		
		SqoopImport.impInfoAllData(impMap);
		
		/*smi.setTable_name("TB_AST_ASSETTYPE2");
		smi.setPrimary_key("CSTM_NO2");
		smi.setTable_type("信息表");
		smi.setPrimary_key("A_ID,BBB_ID");
		SqoopImport.impTableData(smi);*/
		
		
		 
		
	}
	
	
}
