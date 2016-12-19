package com.hiwan.dimp.incremental.bean;

import java.sql.Timestamp;

public class IncrementDate {

	int id ;
	String table_name ;
	String cim_job_name ;
	Timestamp load_date ;
	String file_date ;
	
	public IncrementDate() {
		super();
	}
	public IncrementDate(int id, String table_name, Timestamp load_date,
			String file_date) {
		super();
		this.id = id;
		this.table_name = table_name;
		this.load_date = load_date;
		this.file_date = file_date;
	}
	
	public IncrementDate(int id, String table_name, String cim_job_name,
			Timestamp load_date, String file_date) {
		super();
		this.id = id;
		this.table_name = table_name;
		this.cim_job_name = cim_job_name;
		this.load_date = load_date;
		this.file_date = file_date;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getTable_name() {
		return table_name;
	}
	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}
	public Timestamp getLoad_date() {
		return load_date;
	}
	public void setLoad_date(Timestamp load_date) {
		this.load_date = load_date;
	}
	public String getFile_date() {
		return file_date;
	}
	public void setFile_date(String file_date) {
		this.file_date = file_date;
	}
	public String getCim_job_name() {
		return cim_job_name;
	}
	public void setCim_job_name(String cim_job_name) {
		this.cim_job_name = cim_job_name;
	}
}
