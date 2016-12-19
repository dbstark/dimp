package com.hiwan.dimp.databak.bean;

public class TableBakDayNum {

	String table_name ;
	String bak_day ;
	
	public TableBakDayNum() {
		super();
	}
	public TableBakDayNum(String table_name, String bak_day) {
		super();
		this.table_name = table_name;
		this.bak_day = bak_day;
	}
	public String getTable_name() {
		return table_name;
	}
	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}
	public String getBak_day() {
		return bak_day;
	}
	public void setBak_day(String bak_day) {
		this.bak_day = bak_day;
	}
	
}
