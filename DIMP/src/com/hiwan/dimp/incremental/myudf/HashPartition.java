package com.hiwan.dimp.incremental.myudf;

import org.apache.hadoop.hive.ql.exec.UDF;

public final class HashPartition extends UDF {

	public String evaluate(String partition_column_name , int partition_num){
		String result = "" ;
		if(partition_column_name == null){
			partition_column_name = "default" ;
		}
		if(partition_num == 6){
//			result = "6" ;
			result = partition_6(partition_column_name) ;
		}else{
			long hash_num = Math.abs(partition_column_name.hashCode()) ;
			hash_num = 1 + hash_num%partition_num ;
			result = "P" + hash_num ;
		}
		return result;
	}
	
	public String evaluate(int partition_column_name , int partition_num){
		String result = "" ;
		long num = Math.abs((long)partition_column_name) ;
		if(partition_num == 6){
//			result = "6" ;
			result = partition_6(partition_column_name + "") ;
		}else{
//			int hash_num = Math.abs(partition_column_name.hashCode()) ;
			num = 1 + Math.abs(num%partition_num) ;
			result = "P" + num ;
		}
		return result;
	}
	
	public String evaluate(double partition_column_name , int partition_num){
		String result = "" ;
		long num = Math.abs((long)partition_column_name) ;
		if(partition_num == 6){
			result = "6" ;
			result = partition_6(partition_column_name + "") ;
		}else{
//			int hash_num = Math.abs(partition_column_name.hashCode()) ;
			num =  1 + Math.abs(num%partition_num) ;
			result = "P" + num ;
		}
		return result;
	}
	
	
	public String partition_6(String partition_value){
		int default_num = 205655 ;
		int num = Math.abs(partition_value.hashCode()%6) ;
		int last = default_num + num ;
		return "SYS_P" + last ;
	}
	
}
