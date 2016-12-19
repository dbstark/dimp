package com.hiwan.dimp.incremental.bean;

import java.sql.Timestamp;

public class AugmentLog {

	int augment_id ;
	Timestamp begin_time ;
	Timestamp end_time ;
	String source_path ;
	int source_rownum ;
	int status ;
	
	public AugmentLog() {
		super();
	}
	public AugmentLog(int augment_id, Timestamp begin_time, Timestamp end_time, String source_path, int source_rownum,
			int status) {
		super();
		this.augment_id = augment_id;
		this.begin_time = begin_time;
		this.end_time = end_time;
		this.source_path = source_path;
		this.source_rownum = source_rownum;
		this.status = status;
	}
	
	public int getAugment_id() {
		return augment_id;
	}
	public void setAugment_id(int augment_id) {
		this.augment_id = augment_id;
	}
	public Timestamp getBegin_time() {
		return begin_time;
	}
	public void setBegin_time(Timestamp begin_time) {
		this.begin_time = begin_time;
	}
	public Timestamp getEnd_time() {
		return end_time;
	}
	public void setEnd_time(Timestamp end_time) {
		this.end_time = end_time;
	}
	public String getSource_path() {
		return source_path;
	}
	public void setSource_path(String source_path) {
		this.source_path = source_path;
	}
	public int getSource_rownum() {
		return source_rownum;
	}
	public void setSource_rownum(int source_rownum) {
		this.source_rownum = source_rownum;
	}
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	
}
