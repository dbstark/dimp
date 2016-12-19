package com.hiwan.dimp.incremental.master;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import com.hiwan.dimp.incremental.bean.AugmentInfo;
import com.hiwan.dimp.incremental.bean.AugmentLog;
import com.hiwan.dimp.incremental.bean.AugmentThreadBean;
import com.hiwan.dimp.incremental.bean.SourceFileBean;
import com.hiwan.dimp.incremental.dao.AugmentInfoDao;
import com.hiwan.dimp.incremental.dao.AugmentLogDao;
import com.hiwan.dimp.incremental.dao.JobToTableDao;
import com.hiwan.dimp.incremental.table.ConfigTable;
import com.hiwan.dimp.incremental.table.DetailSummaryTable;
import com.hiwan.dimp.incremental.table.InfoTable;
import com.hiwan.dimp.incremental.util.FileListUtil;
import com.hiwan.dimp.incremental.util.HdfsFileSystem;
import com.hiwan.dimp.incremental.util.HiveConnection2;

public class AugmentMasterThread3 {
	
	public static void main(String[] args) {
		System.out.println("开始时间:"+ new Date());
		if(args == null || args.length < 5){
			System.out.println("args[0]:源文件位置\nargs[1]:源文件转码后正确的目标文件位置\nargs[2]:源文件转码后执行错误的位置\nargs[3]:源文件转码后表不存在的文件位置\nargs[4]:源文件保存位置");
			System.exit(0) ;
		}else{
			try {
				new AugmentMasterThread3().update_data(args);
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
		Map<String, AugmentInfo> aug_info_map = null ;
		//获取job -- table 的map
		Map<String, String> job_table_map = null ;
//		System.out.println("table num:" + aug_info_map.size());
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
		
		//需要数据的默认存放目录  , 不能只有人为设定输出目录
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd") ;
		String path = args[0] ;
		String target_path = args[1] ;
		String error_path = args[2] ;
		String other_path = args[3] ;
		String source_save = args[4] ;
		String date = "" ;
		
		while (true) {
			aug_info_map = aug_info_dao.getAugInfoMap(list) ;
			job_table_map = job_table_dao.job_to_table_map() ;
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
			
//			file_map = file_util.map_data_file(path , job_table_map, source_save_day,aug_info_map) ;
			System.out.println("file_size:" + file_map.size());
			
			LinkedBlockingQueue<AugmentThreadBean> data_queue = new LinkedBlockingQueue<AugmentThreadBean>() ;
			LinkedBlockingQueue<AugmentThreadBean> data_queue2 = new LinkedBlockingQueue<AugmentThreadBean>() ;
			
			for(Entry<String, List<SourceFileBean>> entry : file_map.entrySet()){
				table_name = entry.getKey() ;   //源文件位置
				source_bean = entry.getValue() ; //源文件修改编码后位置
//				System.out.println("table_name:" + table_name);
				aug_info = aug_info_map.get(table_name) ;
				if( aug_info instanceof AugmentInfo ){
					if("6".equals(aug_info.getStatus())){
						data_queue2.add(new AugmentThreadBean(aug_info_map.get(table_name), source_bean)) ;
					}else{
						data_queue.add(new AugmentThreadBean(aug_info_map.get(table_name), source_bean)) ;
					}
				}else{
					file_util.file_mv3(source_bean, other_path_day) ;
//					file_util.file_mv2(source_bean, source_save_day) ;
				}
			}
			int quene_num = data_queue.size() ;
			for(int i = 0 ; i < pool_size && i < quene_num ; i++){
				aug_thread = new AugmentThread(data_queue, target_path_day, error_path_day, other_path_day, source_save_day) ;
				pool.submit(aug_thread) ;
			}
			while(true){
				if( pool.getActiveCount() == 0 ){
					aug_thread = new AugmentThread(data_queue2, target_path_day, error_path_day, other_path_day, source_save_day) ;
					pool.submit(aug_thread) ;
					break ;
				}else{
					Thread.sleep(60*1000);
				}
			}
			break ;
		}
		pool.shutdown(); 
	}
	

	class AugmentThread implements Runnable , Serializable{

		private static final long serialVersionUID = 1L;
		
		private LinkedBlockingQueue<AugmentThreadBean> data_queue ;
		private String target_path_day ;
		private String error_path_day ;
		private String other_path_day ;
		private String source_save_day ;
		
		public AugmentThread(LinkedBlockingQueue<AugmentThreadBean> data_queue, String target_path_day,
				String error_path_day, String other_path_day, String source_save_day) {
			super();
			this.data_queue = data_queue;
			this.target_path_day = target_path_day;
			this.error_path_day = error_path_day;
			this.other_path_day = other_path_day;
			this.source_save_day = source_save_day;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			HiveConnection2 hive_conn = new HiveConnection2() ;
			FileListUtil file_util = new FileListUtil();
			HdfsFileSystem hfs = new HdfsFileSystem() ;
			AugmentThreadBean atb = null ;
			AugmentInfo aug_info = null ;
			List<SourceFileBean> source_bean_list ;
			while(true){
				atb = data_queue.poll() ;
				System.out.println("剩余数据量：" + data_queue.size());
				if( atb instanceof AugmentThreadBean){
					aug_info = atb.getAug_info() ;
					source_bean_list = atb.getSource_bean_list() ;
					if(aug_info instanceof AugmentInfo){
						AugmentLog aug_log = new AugmentLog() ;
						aug_log.setAugment_id(Integer.parseInt(aug_info.getAug_id()));
						aug_log.setBegin_time(new Timestamp(System.currentTimeMillis()));
						boolean where = true ;
						String table_type = aug_info.getTable_type() ;
						String source_target_table_name = "" ;
						int source_rownum = 0 ;
						int status = 0 ;
						try {
							if("配置表".equals(table_type)){
//								ConfigTable.config_table_proces(aug_info, source_bean_list ,hive_conn , hfs) ;
								source_target_table_name = aug_info.getTable_name() + " " ;
							}else if("明细表".equals(table_type) || "汇总表".equals(table_type)){
								DetailSummaryTable.detail_summary_table_proces(aug_info, source_bean_list ,hive_conn,hfs) ;
								if("1".equals(aug_info.getIs_partition())){
									source_target_table_name = aug_info.getMid_table_name() + " " ;
								}else{
									source_target_table_name = aug_info.getTable_name() + " " ;
								}
							}else if("信息表".equals(table_type)){
								InfoTable.config_table_proces(aug_info, source_bean_list ,hive_conn , hfs) ;
								source_target_table_name = aug_info.getMid_table_name() + " " ;
							}
						} catch (Exception e) {
							where = false ;
							System.out.println(e.getMessage());
//							e.printStackTrace();
						}
						ResultSet rs = null ;
						if(where){
							try {
								rs = hive_conn.execute2(" select count(*) from " + source_target_table_name) ;
								source_rownum = rs.getInt(1) ;
								status = 0 ;
								rs.close() ;
							} catch (SQLException e) {
								// TODO Auto-generated catch block
								System.out.println(e.getMessage());
//								e.printStackTrace();
							} 
							file_util.file_mv3(source_bean_list, target_path_day) ;
						}else{
							source_rownum = 0 ;
							status = 1 ;
							file_util.file_mv3(source_bean_list, error_path_day) ;
						}
						aug_log.setEnd_time(new Timestamp(System.currentTimeMillis()));
						aug_log.setSource_path(file_util.list_to_string(source_bean_list, source_save_day));
						aug_log.setSource_rownum(source_rownum);
						aug_log.setStatus(status); //0:正常  1:非正常
						AugmentLogDao.insert_aug_log(aug_log);
					}else{
						file_util.file_mv3(source_bean_list, other_path_day) ;
					}
					//把源文件移动到存储源数据的文件夹中
//					file_util.file_mv2(source_bean_list, source_save_day) ;
				}else{
					break ;
				}
			}
			hive_conn.close(); 
			hfs.colse(); 
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
		public LinkedBlockingQueue<AugmentThreadBean> getData_queue() {
			return data_queue;
		}
		public void setData_queue(LinkedBlockingQueue<AugmentThreadBean> data_queue) {
			this.data_queue = data_queue;
		}
		
		
		
	}
	
}
