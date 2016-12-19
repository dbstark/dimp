package com.hiwan.dimp.incremental.bean;

public class JobToTable {

	String table_name ;
	String job_name ;
	
	public JobToTable(String table_name, String job_name) {
		super();
		this.table_name = table_name;
		this.job_name = job_name;
	}
	
	public String getTable_name() {
		return table_name;
	}
	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}
	public String getJob_name() {
		return job_name;
	}
	public void setJob_name(String job_name) {
		this.job_name = job_name;
	}
	
}
