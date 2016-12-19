package com.hiwan.dimp.tool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PageBean {

	/**
	 * 数据列表
	 */
	List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>(); // 代表实际模型数据的入口
	/**
	 * 当前第几页
	 */
	int page; // json中代表当前页码的数据
	/**
	 * 共多少页
	 */
	int total; // json中代表页码总数的数据
	/**
	 * 记录总数
	 */
	int records;// json中代表数据行总数的数据
	
	public List<Map<String, Object>> getRows() {
		return rows;
	}
	public void setRows(List<Map<String, Object>> rows) {
		this.rows = rows;
	}
	public int getPage() {
		return page;
	}
	public void setPage(int page) {
		this.page = page;
	}
	public int getTotal() {
		return total;
	}
	public void setTotal(int total) {
		this.total = total;
	}
	public int getRecords() {
		return records;
	}
	public void setRecords(int records) {
		this.records = records;
	}
	
	
	
	

}
