package com.hiwan.dimp.incremental.bean;

public class CtlInfo {

	String job_name_date ;
	int file_num ;
	
	public CtlInfo(String job_name_date, int file_num) {
		super();
		this.job_name_date = job_name_date;
		this.file_num = file_num;
	}
	
	public String getJob_name_date() {
		return job_name_date;
	}
	public void setJob_name_date(String job_name_date) {
		this.job_name_date = job_name_date;
	}
	public int getFile_num() {
		return file_num;
	}
	public void setFile_num(int file_num) {
		this.file_num = file_num;
	}

}
