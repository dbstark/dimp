package com.hiwan.dimp.databak.bean;

public class TableBakSql {

	String table_name ;
	String table_sql ;
	String is_partition ;
	
	public TableBakSql() {
		super();
	}
	public TableBakSql(String table_name, String table_sql, String is_partition) {
		super();
		this.table_name = table_name;
		this.table_sql = table_sql;
		this.is_partition = is_partition;
	}
	public String getTable_name() {
		return table_name;
	}
	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}
	public String getTable_sql() {
		return table_sql;
	}
	public void setTable_sql(String table_sql) {
		this.table_sql = table_sql;
	}
	public String getIs_partition() {
		return is_partition;
	}
	public void setIs_partition(String is_partition) {
		this.is_partition = is_partition;
	}
	
}
