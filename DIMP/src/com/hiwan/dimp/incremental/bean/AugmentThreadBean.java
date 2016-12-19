package com.hiwan.dimp.incremental.bean;

import java.util.List;

public class AugmentThreadBean {

	String cim_job_name ;
	AugmentInfo aug_info ;
	List<SourceFileBean> source_bean_list ;
	
	public AugmentThreadBean() {
		super();
	}
	public AugmentThreadBean(AugmentInfo aug_info, List<SourceFileBean> source_bean_list) {
		super();
		this.aug_info = aug_info;
		this.source_bean_list = source_bean_list;
	}

	public AugmentThreadBean(String cim_job_name, AugmentInfo aug_info,
			List<SourceFileBean> source_bean_list) {
		super();
		this.cim_job_name = cim_job_name;
		this.aug_info = aug_info;
		this.source_bean_list = source_bean_list;
	}
	public AugmentInfo getAug_info() {
		return aug_info;
	}
	public void setAug_info(AugmentInfo aug_info) {
		this.aug_info = aug_info;
	}
	public List<SourceFileBean> getSource_bean_list() {
		return source_bean_list;
	}
	public void setSource_bean_list(List<SourceFileBean> source_bean_list) {
		this.source_bean_list = source_bean_list;
	}
	public String getCim_job_name() {
		return cim_job_name;
	}
	public void setCim_job_name(String cim_job_name) {
		this.cim_job_name = cim_job_name;
	}
	
}
