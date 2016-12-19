package com.hiwan.dimp.stock.hash.bean;

import java.util.LinkedHashMap;
import java.util.List;

public class HashTableInfo {

	String table_name ;
	LinkedHashMap<String, String> column_type ;
	List<String> partition_list ;
	
	public HashTableInfo() {
		super();
	}
	
	public String getTable_name() {
		return table_name;
	}
	
	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}
	
	public LinkedHashMap<String, String> getColumn_type() {
		return column_type;
	}
	
	public void setColumn_type(LinkedHashMap<String, String> column_type) {
		this.column_type = column_type;
	}
	
	public List<String> getPartition_list() {
		return partition_list;
	}
	
	public void setPartition_list(List<String> partition_list) {
		this.partition_list = partition_list;
	}
	
}
