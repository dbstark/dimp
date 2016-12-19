package com.hiwan.dimp.incremental.bean;

public class SourceFileBean {

	String file_date ;
	String source_path ;
	String mid_source_path ;
	
	
	public SourceFileBean(String source_path, String mid_source_path) {
		super();
		this.source_path = source_path;
		this.mid_source_path = mid_source_path;
	}
	public SourceFileBean(String file_date, String source_path,
			String mid_source_path) {
		super();
		this.file_date = file_date;
		this.source_path = source_path;
		this.mid_source_path = mid_source_path;
	}

	public String getSource_path() {
		return source_path;
	}
	public void setSource_path(String source_path) {
		this.source_path = source_path;
	}
	public String getMid_source_path() {
		return mid_source_path;
	}
	public void setMid_source_path(String mid_source_path) {
		this.mid_source_path = mid_source_path;
	}
	public String getFile_date() {
		return file_date;
	}
	public void setFile_date(String file_date) {
		this.file_date = file_date;
	}
	
}
