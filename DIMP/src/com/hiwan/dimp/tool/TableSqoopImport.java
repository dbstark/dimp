package com.hiwan.dimp.tool;

import com.hiwan.dimp.db.StreamGobbler;

public class TableSqoopImport {

	public void table_sqoop_import( String command ) throws Exception{
		Process process = Runtime.getRuntime().exec(command);
		StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "ERROR","CPDDS_ETL.int_fields_list");
		errorGobbler.start(); 
		StreamGobbler outGobbler = new StreamGobbler(process.getInputStream(), "STDOUT","CPDDS_ETL.int_fields_list");
		outGobbler.start(); 
//		int n = process.waitFor(); 
	}
	
	public void three_table_import(){
		String command = "" ;
		try {
			command = "sqoop import -D oraoop.block.allocation=RANDOM -D oracle.row.fetch.size=10000 " +
					" --direct --connect jdbc:oracle:thin:@21.144.56.131:1521:orcl1 --username HADOOP --password hadoop " +
					" --table CPDDS_ETL.int_fields_list --warehouse-dir hdfs://nameservice1/inceptorsql1/user/hive/warehouse/mpm.db/mpm/ " +
					" --fields-terminated-by '!'  --delete-target-dir --num-mappers 3" ;
			table_sqoop_import(command) ;
			command = "sqoop import -D oraoop.block.allocation=RANDOM -D oracle.row.fetch.size=10000 " +
					" --direct --connect jdbc:oracle:thin:@21.144.56.131:1521:orcl1 --username HADOOP --password hadoop " +
					" --table CPDDS_ETL.int_info --warehouse-dir hdfs://nameservice1/inceptorsql1/user/hive/warehouse/mpm.db/mpm/ " +
					" --fields-terminated-by '!'  --delete-target-dir --num-mappers 3" ;
			table_sqoop_import(command) ;
			command = "sqoop import -D oraoop.block.allocation=RANDOM -D oracle.row.fetch.size=10000 " +
					" --direct --connect jdbc:oracle:thin:@21.144.56.131:1521:orcl1 --username HADOOP --password hadoop " +
					" --table CPDDS_ETL.src_sys --warehouse-dir hdfs://nameservice1/inceptorsql1/user/hive/warehouse/mpm.db/mpm/ " +
					" --fields-terminated-by '!'  --delete-target-dir --num-mappers 3" ;
			table_sqoop_import(command) ;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		TableSqoopImport tsi = new TableSqoopImport() ;
		tsi.three_table_import() ;
	}

}
