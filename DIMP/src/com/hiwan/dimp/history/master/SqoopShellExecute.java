package com.hiwan.dimp.history.master;

import java.util.Map;
import java.util.Properties;

import com.hiwan.dimp.db.StreamGobbler;
import com.hiwan.dimp.history.util.HistoryDBAccess;

public class SqoopShellExecute {

	private static String url;
	private static String username;
	private static String password;

	static {
		Properties prop = new Properties();
		try {
			prop.load(HistoryDBAccess.class.getClassLoader().getResourceAsStream("history_oracle_config.properties"));
			url = prop.getProperty("url");
			username = prop.getProperty("username");
			password = prop.getProperty("password");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void partition_table_sqoop(Map<String, String> map) { // 注意oracle中的表名和hive中不同
		String num0 = "50";
		Integer num = Integer.valueOf(num0.trim()) <= 3 ? 3 : Integer.valueOf(num0.trim());

		int n = 1;
		try {
			StringBuilder command = new StringBuilder("");
			command.append("sqoop import -D oraoop.block.allocation=RANDOM -D oracle.row.fetch.size=10000 ").append("-D oraoop.import.partitions="+map.get("partition_value")+"")									
			.append(" --direct  --connect ").append(url).append(" --username ").append(username.toUpperCase()).append(" --password ").append(password)	
			.append(" --table ").append(/*map.get("view") + "." +*/ map.get("oracle_name").toString().trim().toUpperCase())
			.append(" --hive-import --hive-table ").append(map.get("hive_name").toString().trim().toUpperCase())
			.append(" --hive-partition-key "+map.get("partition_name")) //
			.append(" --hive-partition-value "+map.get("partition_value"))
			.append(" --target-dir /user/root/"+map.get("hive_name").toString().trim().toUpperCase()+"_"+map.get("partition_value"))
			.append(" --fields-terminated-by ! --delete-target-dir --num-mappers "+num+" ");
			System.out.println(" -------- sqoop import -------- :"+command.toString());
			Process process = Runtime.getRuntime().exec(command.toString());
			StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "ERROR",map.get("hive_name").toString().trim().toUpperCase()+"_"+map.get("partition_value"));
			errorGobbler.start(); 
			StreamGobbler outGobbler = new StreamGobbler(process.getInputStream(), "STDOUT",map.get("hive_name").toString().trim().toUpperCase()+"_"+map.get("partition_value"));
			outGobbler.start();
			// 正常结束0 异常1
			n = process.waitFor();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public void not_partition_table_sqoop(Map<String, String> map) {
		String num = "50";
		int n = 1;
		try {
			StringBuilder command = new StringBuilder("");
			command.append("sqoop import -D oraoop.block.allocation=RANDOM -D oracle.row.fetch.size=10000 --direct --connect ")
		    .append(url).append(" --username ").append(username.toUpperCase()).append(" --password ").append(password)	
			.append(" --table ").append(/*map.get("view") + "." +*/ map.get("oracle_name").trim().toUpperCase())
			.append(" --target-dir /user/root/").append(map.get("hive_name").trim().toUpperCase())
			.append(" --hive-import --hive-table ").append(map.get("hive_name").trim().toUpperCase())
			.append(" --fields-terminated-by ! --hive-overwrite --delete-target-dir --num-mappers ").append(num);
			System.out.println("sqoop import -------- :"+command.toString());
			Process process = Runtime.getRuntime().exec(command.toString());
			StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "ERROR",map.get("hive_name").toString().trim().toUpperCase());
			errorGobbler.start(); 
			StreamGobbler outGobbler = new StreamGobbler(process.getInputStream(), "STDOUT",map.get("hive_name").toString().trim().toUpperCase());
			outGobbler.start();
			// 正常结束0 异常1
			n = process.waitFor();
		} catch (Exception e) {
			e.printStackTrace();
	 	}
	}

}
