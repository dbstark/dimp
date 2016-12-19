package com.hiwan.dimp.bean;

public class MataStockLog {
	private String id ;
	private String stock_id ;
	private String partion_id;
	private String begin_time ;
	private String end_time ;
	private String source_rownum ;
	private String target_rownum ;
	private String status ;
	
	public String getPartion_id() {
		return partion_id;
	}
	public void setPartion_id(String partion_id) {
		this.partion_id = partion_id;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getStock_id() {
		return stock_id;
	}
	public void setStock_id(String stock_id) {
		this.stock_id = stock_id;
	}
	public String getBegin_time() {
		return begin_time;
	}
	public void setBegin_time(String begin_time) {
		this.begin_time = begin_time;
	}
	public String getEnd_time() {
		return end_time;
	}
	public void setEnd_time(String end_time) {
		this.end_time = end_time;
	}
	public String getSource_rownum() {
		return source_rownum;
	}
	public void setSource_rownum(String source_rownum) {
		this.source_rownum = source_rownum;
	}
	public String getTarget_rownum() {
		return target_rownum;
	}
	public void setTarget_rownum(String target_rownum) {
		this.target_rownum = target_rownum;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	@Override
	public String toString() {
		return "MataStockLog [id=" + id + ", stock_id=" + stock_id
				+ ", partion_id=" + partion_id + ", begin_time=" + begin_time
				+ ", end_time=" + end_time + ", source_rownum=" + source_rownum
				+ ", target_rownum=" + target_rownum + ", status=" + status
				+ "]";
	}
	
	
	
	
}
