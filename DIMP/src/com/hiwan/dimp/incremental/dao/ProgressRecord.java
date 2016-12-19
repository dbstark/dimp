package com.hiwan.dimp.incremental.dao;

/**
 * @author CHENGKAI.SHENG
 * @since 2016-12-15
 * 修改记录：
 * 	 # 2016-12-15，新建类ProgressRecord
 *
 */
public class ProgressRecord {
	/**
	 * 以下private字段与表tb_file_loading_progress字段对应关系如下：
	 * jobName 	- job_name, 文件对应的job名称，数据源1个表对应n个job（业务），一个job对应n个file（一个job数据split成n个file）
	 * fileName - file_name, 文件名
	 * loadType - load_type, 加载方式，F（全量）、A（追加）、U（update, merge实现）
	 * progressPercent - progress_percent, 文件完成加载的百分比
	 * isSucessful - is_successful, 是否加载成功，1为处理中，2为成功，3为意外中断，4为异常
	 * loadInof - load_info, 如果加载出现错误，错误的详细信息
	 * startTime - start_time, 文件加载开始时间
	 * endTime - end_time, 文件加载结束时间
	 */
	public String jobName;
	public String fileName;
	public String loadType;
	public double progressPercent;
	public int isSucessful;
	public String loadInfo;
	public String startTime;
	public String endTime;
	
	public ProgressRecord(String jobName,
						  String fileName,
						  String loadType,
						  double progressPercent,
						  int isSuccessful,
						  String loadInfo,
						  String startTime,
						  String endTime) {
		this.jobName = jobName;
		this.fileName = fileName;
		this.loadType = loadType;
		this.progressPercent = progressPercent;
		this.isSucessful = isSuccessful;
		this.loadInfo = loadInfo;
		this.startTime = startTime;
		this.endTime = endTime;
	}
}
