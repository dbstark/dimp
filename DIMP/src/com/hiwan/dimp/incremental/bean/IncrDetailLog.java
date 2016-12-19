package com.hiwan.dimp.incremental.bean;

public class IncrDetailLog {

	String id ;
	String table_name ;
	String job_name ;
	String job_type ;
	String file_date ;
	String file_list ;
	int file_num ;
	String convertstatus ;
	String sdatastatus ;
	String pdatastatus ;
	String begintime ;
	String endtime ;
	String mark ;
	

	public IncrDetailLog() {
		super();
	}
	
	public IncrDetailLog(String id, String table_name, String job_name,
			String job_type, String file_date, String file_list, int file_num,
			String convertstatus, String sdatastatus, String pdatastatus,
			String begintime, String endtime, String mark) {
		super();
		this.id = id;
		this.table_name = table_name;
		this.job_name = job_name;
		this.job_type = job_type;
		this.file_date = file_date;
		this.file_list = file_list;
		this.file_num = file_num;
		this.convertstatus = convertstatus;
		this.sdatastatus = sdatastatus;
		this.pdatastatus = pdatastatus;
		this.begintime = begintime;
		this.endtime = endtime;
		this.mark = mark;
	}
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
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
	public String getJob_type() {
		return job_type;
	}
	public void setJob_type(String job_type) {
		this.job_type = job_type;
	}
	public String getFile_date() {
		return file_date;
	}
	public void setFile_date(String file_date) {
		this.file_date = file_date;
	}
	public String getFile_list() {
		return file_list;
	}
	public void setFile_list(String file_list) {
		this.file_list = file_list;
	}
	public int getFile_num() {
		return file_num;
	}
	public void setFile_num(int file_num) {
		this.file_num = file_num;
	}
	public String getConvertstatus() {
		return convertstatus;
	}
	public void setConvertstatus(String convertstatus) {
		this.convertstatus = convertstatus;
	}
	public String getSdatastatus() {
		return sdatastatus;
	}
	public void setSdatastatus(String sdatastatus) {
		this.sdatastatus = sdatastatus;
	}
	public String getPdatastatus() {
		return pdatastatus;
	}
	public void setPdatastatus(String pdatastatus) {
		this.pdatastatus = pdatastatus;
	}
	public String getBegintime() {
		return begintime;
	}
	public void setBegintime(String begintime) {
		this.begintime = begintime;
	}
	public String getEndtime() {
		return endtime;
	}
	public void setEndtime(String endtime) {
		this.endtime = endtime;
	}
	public String getMark() {
		return mark;
	}
	public void setMark(String mark) {
		this.mark = mark;
	}
	
	
}
