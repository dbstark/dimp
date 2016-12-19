package com.hiwan.dimp.db;

import java.util.Map;

import com.hiwan.dimp.hive.HiveUtils;

public class IncrementImport {
	public static void impTableData(Map<String, String> map) {
		try {
			if (map.get("tableType").equals("配置表")) {
				configure(map);
			}
			if (map.get("tableType").equals("信息表")) {
				information(map);
			}
			if (map.get("tableType").equals("汇总表")) {
				detailAndSum(map);
			}
			if (map.get("tableType").equals("明细表")) {
				detailAndSum(map);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 增量 数据导入  信息表
			1.truncate mid_table(普通hive表)
			2.加载数据文件到mid_table所在hdfs目录中(注意分区表)
			3.对mid_table根据主键进行去重操作
			4.把数据有mid_table导入target_table(merge into)
			
			
	 */
	private static void information(Map<String, String> map) {
		HiveUtils hu=new HiveUtils();
		hu.truncateTable(map.get("tableName")+"add");
		hu.close();
		try{
			// hadoop fs -put // /inceptorsql1/user/hive/warehouse/table_name
			StringBuilder command = new StringBuilder("");
			command.append("hadoop fs -put ")
				   .append(map.get("localPath"))
				   .append(" /inceptorsql1/user/hive/warehouse/")
				   .append(map.get("tableName")+"add");
			System.out.println(command.toString());
			//linux 命令
			Process process = Runtime.getRuntime().exec(command.toString());
			//windows 
			//Process process = Runtime.getRuntime().exec(new String[]{"notepad.exe","F:\\long_cennect.txt"});
			//StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "ERROR");
			//errorGobbler.start(); 
			//StreamGobbler outGobbler = new StreamGobbler(process.getInputStream(), "STDOUT");
			//outGobbler.start(); 
			process.waitFor(); 

		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	/**
	 * 增量 配置表 数据导入
	 * 1.truncate target_table(普通hive表) ; 
	 * 2.加载文件到target_table所在hdfs目录位置(注意分区表)
	 */
	private static  void  configure(Map<String ,String> map){
		HiveUtils hu=new HiveUtils();
		hu.truncateTable(map.get("tableName"));
		hu.close();
		
		try{
			// hadoop fs -put // /inceptorsql1/user/hive/warehouse/table_name
			StringBuilder command = new StringBuilder("");
			command.append("hadoop fs -put ")
			   .append(map.get("localPath"))
			   .append(" /inceptorsql1/user/hive/warehouse/")
			   .append(map.get("tableName"));
			System.out.println(command.toString());
			//linux 命令
			Process process = Runtime.getRuntime().exec(command.toString());
			//windows 
			//Process process = Runtime.getRuntime().exec(new String[]{"notepad.exe","F:\\long_cennect.txt"});
		//	StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "ERROR");
			//errorGobbler.start(); 
			//StreamGobbler outGobbler = new StreamGobbler(process.getInputStream(), "STDOUT");
			///outGobbler.start(); 
			process.waitFor(); 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 增量  数据导入  明细表和汇总表
	 * 1.truncate target_table(普通hive表) ; 
	 * 2.加载文件到target_table所在hdfs目录位置(注意分区表)
	 */
	private static void detailAndSum(Map<String ,String> map){
		try{
			// hadoop fs -put // /inceptorsql1/user/hive/warehouse/table_name
			StringBuilder command = new StringBuilder("");
			command.append("hadoop fs -put ").append(map.get("localPath"))
			       .append(" /inceptorsql1/user/hive/warehouse/").append(map.get("tableName"));
			System.out.println(command.toString());
			//linux 命令
			Process process = Runtime.getRuntime().exec(command.toString());
			//windows 
			//Process process = Runtime.getRuntime().exec(new String[]{"notepad.exe","F:\\long_cennect.txt"});
			/*StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "ERROR");
			errorGobbler.start(); 
			StreamGobbler outGobbler = new StreamGobbler(process.getInputStream(), "STDOUT");
			outGobbler.start(); */
			process.waitFor(); 

		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
