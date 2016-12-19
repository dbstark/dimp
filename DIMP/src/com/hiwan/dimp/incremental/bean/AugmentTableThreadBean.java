package com.hiwan.dimp.incremental.bean;

import java.util.List;

public class AugmentTableThreadBean {

	String table_name ;
	List<AugmentThreadBean> list ;

	
	public AugmentTableThreadBean() {
		super();
	}

	public AugmentTableThreadBean(String table_name,
			List<AugmentThreadBean> list) {
		super();
		this.table_name = table_name;
		this.list = list;
	}

	public String getTable_name() {
		return table_name;
	}

	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}

	public List<AugmentThreadBean> getList() {
		return list;
	}

	public void setList(List<AugmentThreadBean> list) {
		this.list = list;
	}
	
}
