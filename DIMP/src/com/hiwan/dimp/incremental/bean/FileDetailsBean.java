package com.hiwan.dimp.incremental.bean;

public class FileDetailsBean {

	int abnormal_line_num ;
	int total_line_num ;
	double abnormal_proportion ;
	String right_wrong ;
	
	public FileDetailsBean() {
		super();
	}
	public FileDetailsBean(int abnormal_line_num, int total_line_num,
			double abnormal_proportion, String right_wrong) {
		super();
		this.abnormal_line_num = abnormal_line_num;
		this.total_line_num = total_line_num;
		this.abnormal_proportion = abnormal_proportion;
		this.right_wrong = right_wrong;
	}
	
	public int getAbnormal_line_num() {
		return abnormal_line_num;
	}
	public void setAbnormal_line_num(int abnormal_line_num) {
		this.abnormal_line_num = abnormal_line_num;
	}
	public int getTotal_line_num() {
		return total_line_num;
	}
	public void setTotal_line_num(int total_line_num) {
		this.total_line_num = total_line_num;
	}
	public double getAbnormal_proportion() {
		return abnormal_proportion;
	}
	public void setAbnormal_proportion(double abnormal_proportion) {
		this.abnormal_proportion = abnormal_proportion;
	}
	public String getRight_wrong() {
		return right_wrong;
	}
	public void setRight_wrong(String right_wrong) {
		this.right_wrong = right_wrong;
	}
	
}
