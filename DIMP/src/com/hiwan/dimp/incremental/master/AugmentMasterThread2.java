package com.hiwan.dimp.incremental.master;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.hiwan.dimp.incremental.bean.AugmentInfo;
import com.hiwan.dimp.incremental.bean.AugmentLog;
import com.hiwan.dimp.incremental.bean.SourceFileBean;
import com.hiwan.dimp.incremental.dao.AugmentInfoDao;
import com.hiwan.dimp.incremental.dao.JobToTableDao;
import com.hiwan.dimp.incremental.table.ConfigTable;
import com.hiwan.dimp.incremental.table.DetailSummaryTable;
import com.hiwan.dimp.incremental.table.InfoTable;
import com.hiwan.dimp.incremental.util.FileListUtil;
import com.hiwan.dimp.incremental.util.HiveConnection2;

public class AugmentMasterThread2 {
	
	public static void main(String[] args) {
		System.out.println("开始时间:"+ new Date());
		if(args.length < 5){
			System.out.println("args[0]:源文件位置\nargs[1]:源文件转码后正确的目标文件位置\nargs[2]:源文件转码后执行错误的位置\nargs[3]:源文件转码后表不存在的文件位置\nargs[4]:源文件保存位置");
			System.exit(0) ;
		}else{
			try {
				new AugmentMasterThread2().update_data(args);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("结束时间:"+ new Date());
	}
	
	public void update_data(String[] args ) throws Exception{
		AugmentInfoDao aug_info_dao = new AugmentInfoDao() ;
		JobToTableDao job_table_dao = new JobToTableDao() ;
		FileListUtil file_util = new FileListUtil();
		//需要哪些条件,添加到map中
//		Map<String, String> condition_map = new HashMap<String, String>() ;
		List<String> list = new ArrayList<String>() ;
		//获取table_name - aug_info 的 map
		Map<String, AugmentInfo> aug_info_map = aug_info_dao.getAugInfoMap(list) ;
		//获取job -- table 的map
		Map<String, String> job_table_map = job_table_dao.job_to_table_map() ;
		System.out.println("table num:" + aug_info_map.size());
		//线程池  -- 暂时定义线程池线程数为8
		int pool_size = 8 ;
		ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(pool_size);
		AugmentThread aug_thread = null ;
		/**
		 * 以数据文件所在的文件夹中的内容为循环主体
		 * 设计称为死循环的方式,定时对文件夹进行扫描
		 * */
		Map<String, List<SourceFileBean>> file_map = null ;
		String table_name = null ;
		List<SourceFileBean> source_bean = null ;
		AugmentInfo aug_info = null ;
//		HiveConnection hive_conn = new HiveConnection() ;
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd") ;
		String path = args[0] ;
		String target_path = args[1] ;
		String error_path = args[2] ;
		String other_path = args[3] ;
		String source_save = args[4] ;
		String date = "" ;
		
		while (true) {
			/**
			 * 对输出文件夹加上执行的日期
			 * */
			date = sdf.format(new Date()) ;
			String target_path_day = target_path + "/" + date ;
			String error_path_day = error_path +  "/" + date ;
			String other_path_day = other_path + "/" + date ;
			String source_save_day = source_save + "/" + date ;
			file_util.create_directory(target_path_day) ;
			file_util.create_directory(error_path_day) ;
			file_util.create_directory(other_path_day) ;
			file_util.create_directory(source_save_day) ;
			
//			file_map = file_util.map_data_file(path , job_table_map , source_save_day ,aug_info_map) ;
			System.out.println("file_size:" + file_map.size());
			for(Entry<String, List<SourceFileBean>> entry : file_map.entrySet()){
				table_name = entry.getKey() ;   //源文件位置
				source_bean = entry.getValue() ; //源文件修改编码后位置
				
//				String file_path = entry.getKey() ;   //源文件位置
//				String file_code_change_path = entry.getValue() ; //源文件修改编码后位置
				
//				int start = file_path.indexOf("_") ;
//				int end = file_path.indexOf("3200") ;
//				if( start > -1 && end > -1 ){
//					table_name = file_path.substring(file_path.indexOf("_")+1, file_path.indexOf("3200")-1) ;
//				}else{
//					table_name = "null" ;
//				}
				System.out.println("table_name:" + table_name);
				System.out.println(new Date());
				aug_info = aug_info_map.get(table_name) ;
				while(true){
					if(pool.getActiveCount() > pool_size-1){
						System.out.println("pool size:" + pool.getActiveCount());
						Thread.sleep(10*1000);
					}else{
						break ;
					}
				}
//				aug_thread = new AugmentThread( aug_info, target_path_day, error_path_day, other_path_day, source_save_day, file_path, file_code_change_path) ;
				aug_thread = new AugmentThread( aug_info, target_path_day, error_path_day, other_path_day, source_save_day, source_bean) ;
				pool.submit(aug_thread) ;
			}
			
			break ;
		}
		pool.shutdown(); 
//		if(hive_conn != null){
//			hive_conn.close() ;
//		}
	}
	
	
	

	class AugmentThread implements Runnable , Serializable{

		private static final long serialVersionUID = 1L;
		
		private AugmentInfo aug_info ;
		private String target_path_day ;
		private String error_path_day ;
		private String other_path_day ;
		private String source_save_day ;
		private List<SourceFileBean> source_bean ;

		public AugmentThread(AugmentInfo aug_info, String target_path_day, String error_path_day, String other_path_day,
				String source_save_day, List<SourceFileBean> source_bean) {
			super();
			this.aug_info = aug_info;
			this.target_path_day = target_path_day;
			this.error_path_day = error_path_day;
			this.other_path_day = other_path_day;
			this.source_save_day = source_save_day;
			this.source_bean = source_bean ;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			try {
				HiveConnection2 hive_conn = new HiveConnection2() ;
				FileListUtil file_util = new FileListUtil();
				if(aug_info instanceof AugmentInfo){
					AugmentLog aug_log = new AugmentLog() ;
					aug_log.setAugment_id(Integer.parseInt(aug_info.getAug_id()));
					aug_log.setBegin_time(new Timestamp(System.currentTimeMillis()));
					boolean where = false ;
					String table_type = aug_info.getTable_type() ;
					String source_target_table_name = "" ;
					int source_rownum = 0 ;
					int status = 0 ;
					if("配置表".equals(table_type)){
						where = ConfigTable.config_table_proces(aug_info, source_bean ,hive_conn) ;
						source_target_table_name = aug_info.getTable_name() + " " ;
					}else if("明细表".equals(table_type) || "汇总表".equals(table_type)){
						where = DetailSummaryTable.detail_summary_table_proces(aug_info, source_bean ,hive_conn) ;
						if("1".equals(aug_info.getIs_partition())){
							source_target_table_name = aug_info.getMid_table_name() + " " ;
						}else{
							source_target_table_name = aug_info.getTable_name() + " " ;
						}
					}else if("信息表".equals(table_type)){
						where = InfoTable.config_table_proces(aug_info, source_bean ,hive_conn) ;
						source_target_table_name = aug_info.getMid_table_name() + " " ;
//						file_util.file_mv(file_code_change_path, other_path_day) ;
//						file_util.file_delete(file_code_change_path) ;
					}
					ResultSet rs = null ;
					if(where){
						rs = hive_conn.execute2(" select count(*) from " + source_target_table_name) ;
						source_rownum = rs.getInt(1) ;
						status = 0 ;
						rs.close() ; 
//						file_util.file_mv(file_path, target_path_day) ;
//						file_util.file_mv(file_code_change_path, target_path_day) ;
						file_util.file_mv(source_bean, target_path_day) ;
					}else{
						source_rownum = 0 ;
						status = 1 ;
//						file_util.file_mv(file_path, error_path_day) ;
//						file_util.file_mv(file_code_change_path, error_path_day) ;
						file_util.file_mv(source_bean, error_path_day) ;
					}
					aug_log.setEnd_time(new Timestamp(System.currentTimeMillis()));
					aug_log.setSource_path(file_util.list_to_string(source_bean, source_save_day));
					aug_log.setSource_rownum(source_rownum);
					aug_log.setStatus(status); //0:正常  1:非正常
//					AugmentLogDao.insert_aug_log(aug_log);
				}else{
//					file_util.file_mv(file_path, other_path_day) ;
//					file_util.file_mv(file_code_change_path, other_path_day) ;
					file_util.file_mv(source_bean, other_path_day) ;
				}
				//把源文件移动到存储源数据的文件夹中
//				file_util.file_delete(file_code_change_path) ;
//				file_util.file_mv(file_path, source_save_day) ;
//				file_util.file_mv(source_bean, source_save_day) ;
//				file_util.file_delete(file_path) ;
				hive_conn.close(); 
//				System.out.println("当前时间:" + new Date()); 
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
		
		

		public AugmentInfo getAug_info() {
			return aug_info;
		}
		public void setAug_info(AugmentInfo aug_info) {
			this.aug_info = aug_info;
		}
		public String getTarget_path_day() {
			return target_path_day;
		}
		public void setTarget_path_day(String target_path_day) {
			this.target_path_day = target_path_day;
		}
		public String getError_path_day() {
			return error_path_day;
		}
		public void setError_path_day(String error_path_day) {
			this.error_path_day = error_path_day;
		}
		public String getOther_path_day() {
			return other_path_day;
		}
		public void setOther_path_day(String other_path_day) {
			this.other_path_day = other_path_day;
		}
		public String getSource_save_day() {
			return source_save_day;
		}
		public void setSource_save_day(String source_save_day) {
			this.source_save_day = source_save_day;
		}
		public List<SourceFileBean> getSource_bean() {
			return source_bean;
		}
		public void setSource_bean(List<SourceFileBean> source_bean) {
			this.source_bean = source_bean;
		}
		
		
	}
	
}
