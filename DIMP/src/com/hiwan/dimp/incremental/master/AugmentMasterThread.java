package com.hiwan.dimp.incremental.master;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import com.hiwan.dimp.databak.dao.DataLoadBakTable;
import com.hiwan.dimp.databak.dao.TableBakDayNumDao;
import com.hiwan.dimp.incremental.bean.AugIncrMapInfo;
import com.hiwan.dimp.incremental.bean.AugmentInfo;
import com.hiwan.dimp.incremental.bean.AugmentLog;
import com.hiwan.dimp.incremental.bean.AugmentTableThreadBean;
import com.hiwan.dimp.incremental.bean.AugmentThreadBean;
import com.hiwan.dimp.incremental.bean.IncrDetailLog;
import com.hiwan.dimp.incremental.bean.IncrementDate;
import com.hiwan.dimp.incremental.bean.SourceFileBean;
import com.hiwan.dimp.incremental.dao.AugmentInfoDao;
import com.hiwan.dimp.incremental.dao.AugmentLogDao;
import com.hiwan.dimp.incremental.dao.IncrDetailLogDao;
import com.hiwan.dimp.incremental.dao.IncrementDateDao;
import com.hiwan.dimp.incremental.dao.JobToTableDao;
import com.hiwan.dimp.incremental.table.ConfigTable;
import com.hiwan.dimp.incremental.table.DetailSummaryTable;
import com.hiwan.dimp.incremental.table.InfoTable;
import com.hiwan.dimp.incremental.util.FileListUtil;
import com.hiwan.dimp.incremental.util.HbaseConnection;
import com.hiwan.dimp.incremental.util.HdfsFileSystem;
import com.hiwan.dimp.incremental.util.HiveConnection2;

public class AugmentMasterThread {
	
	public static void main(String[] args) throws Exception {
		System.out.println("开始时间:"+ new Date());
		String[] cmd = {
				"/bin/sh",
				"-c",
				"ps -ef | grep AugmentMasterThread"
				};
		Process process = Runtime.getRuntime().exec(cmd);
		BufferedInputStream bis = new BufferedInputStream(process.getInputStream());
		BufferedReader br = new BufferedReader(new InputStreamReader(bis));
		String progress = "" ;
		String line = "" ;
		while(( line = br.readLine() ) != null ) {
			if(line.contains("grep AugmentMasterThread")){
				continue ;
			}
			progress += line  ;
		}
		if(progress.split("AugmentMasterThread").length > 2 ){
			System.out.println("需要启动的程序正在运行,退出");
			System.exit(0) ;
		}
		if(args == null || args.length < 5){
			System.out.println("args[0]:源文件位置\nargs[1]:源文件转码后正确的目标文件位置\nargs[2]:源文件转码后执行错误的位置\nargs[3]:源文件转码后表不存在的文件位置\nargs[4]:源文件保存位置");
			System.exit(0) ;
		}else{
			try {
				new AugmentMasterThread().update_data(args);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("结束时间:"+ new Date());
	}
	
	public void update_data(String[] args ) {
		
		AugmentInfoDao aug_info_dao = new AugmentInfoDao() ;
		JobToTableDao job_table_dao = new JobToTableDao() ;
		IncrementDateDao incre_date_dao = new IncrementDateDao() ;
		TableBakDayNumDao bay_day_dao = new TableBakDayNumDao() ;
		
		FileListUtil file_util = new FileListUtil();
		//需要哪些条件,添加到map中
		List<String> list = new ArrayList<String>() ;
		//获取table_name - aug_info 的 map
		Map<String, AugmentInfo> aug_info_map = null ;
		//获取job -- table 的map
		Map<String, String> job_table_map = null ;
		//获取 tablename -- incrementdate
		Map<String, IncrementDate> incre_date_map = null ;
		//获取数据备份的表和对应的天数
		Map<String, Integer> bak_day_map = null ;
		
		//线程池  -- 暂时定义线程池线程数为8
		int pool_size = 9 ;
		ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(pool_size);
		AugmentThread aug_thread = null ;
		/**
		 * 以数据文件所在的文件夹中的内容为循环主体
		 * 设计称为死循环的方式,定时对文件夹进行扫描
		 * */
		/**
		 * 格式：cim_job -- file_list
		 * */
		AugIncrMapInfo aimi = null ;
		Map<String, List<SourceFileBean>> file_map = null ;
		Map<String, IncrDetailLog> incr_detail_map = null ;
		String table_name = null ;
		String cim_job_name = null ;
		List<SourceFileBean> source_bean = null ;
		AugmentInfo aug_info = null ;
		
		//需要数据的默认存放目录  , 不能只有人为设定输出目录
		String source_path = args[0] ;
		String target_path = args[1] ;
		String error_path = args[2] ;
		String other_path = args[3] ;
		String source_save = args[4] ;
		
//		String abnormal_path = args[5] ;
		String abnormal_path = "/home/jshdata_increment/data/abnormal" ;
		
		while (true) {
			try {
				// @stage [1]
				aug_info_map = aug_info_dao.getAugInfoMap(list) ;
				// @stage [2]
				/*** cim_job_name --- table_name* */
				job_table_map = job_table_dao.job_table_map_from_hive();
				// @stage [3]
				//获得job-IncrementDate的map
				incre_date_map = incre_date_dao.getIncrementJobDate() ;
				// @stage [4]
				bak_day_map = bay_day_dao.table_day() ;
				// @stage [5]
				aimi = file_util.map_data_file(source_path , job_table_map, other_path,abnormal_path,aug_info_map,incre_date_map) ;
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				System.exit(0) ;
			}
			
			file_map = aimi.getMap_file() ;
			incr_detail_map = aimi.getIncr_detail_map() ;
			System.out.println("插入明细表Map大小:" + incr_detail_map.size());
			System.out.println("file_size:" + file_map.size());
			
			LinkedBlockingQueue<AugmentTableThreadBean> table_data_queue = new LinkedBlockingQueue<AugmentTableThreadBean>() ;
			LinkedBlockingQueue<AugmentTableThreadBean> table_data_queue2 = new LinkedBlockingQueue<AugmentTableThreadBean>() ;
			LinkedBlockingQueue<AugmentTableThreadBean> table_data_queue3 = new LinkedBlockingQueue<AugmentTableThreadBean>() ;
			
			Map<String, List<AugmentThreadBean>> table_name_cim_job_files_map = new HashMap<String, List<AugmentThreadBean>>() ;
			List<AugmentThreadBean> exists_job_list = null ;
			for(Entry<String, List<SourceFileBean>> entry : file_map.entrySet()){
				cim_job_name = entry.getKey() ;   //源文件位置
				if(job_table_map.containsKey(cim_job_name)){
					table_name = job_table_map.get(cim_job_name) ;
				}
				source_bean = entry.getValue() ; //源文件修改编码后位置
				aug_info = aug_info_map.get(table_name) ;
				if( aug_info instanceof AugmentInfo ){
					if(table_name_cim_job_files_map.containsKey(table_name)){
						exists_job_list = table_name_cim_job_files_map.get(table_name) ;
					}else{
						exists_job_list = new ArrayList<AugmentThreadBean>() ;
					}
					exists_job_list.add(new AugmentThreadBean(cim_job_name , aug_info_map.get(table_name), source_bean)) ;
					table_name_cim_job_files_map.put(table_name, exists_job_list) ;
				}/*else {
					file_util.file_mv3(source_bean, other_path_day) ;
				}*/
			}
			
			for(Entry<String, List<AugmentThreadBean>> entry : table_name_cim_job_files_map.entrySet()){
				table_name = entry.getKey() ;
				aug_info = aug_info_map.get(table_name) ;
				if("TCS_PARTY_AGMT_RELA_H".equals(table_name)){
					table_data_queue3.add(new AugmentTableThreadBean(table_name, entry.getValue())) ;
				}else if("6".equals(aug_info.getStatus())){
					table_data_queue2.add(new AugmentTableThreadBean(table_name, entry.getValue())) ;
				}else{
					table_data_queue.add(new AugmentTableThreadBean(table_name, entry.getValue())) ;
				}
			}
			
			System.out.println("quene_num:" + table_data_queue.size());
			int quene_num = table_data_queue.size() ;
			for(int i = 0 ; i < pool_size && i < quene_num ; i++){
				if(i == 0){
					aug_thread = new AugmentThread(table_data_queue3, target_path, error_path, other_path, source_save,incre_date_map , bak_day_map,incr_detail_map) ;
				}else{
					aug_thread = new AugmentThread(table_data_queue, target_path, error_path, other_path, source_save,incre_date_map , bak_day_map,incr_detail_map) ;
				}
				pool.submit(aug_thread) ;
			}

			while(true){
				if( pool.getActiveCount() == 0 ){
					aug_thread = new AugmentThread(table_data_queue2, target_path, error_path, other_path, source_save,incre_date_map , bak_day_map,incr_detail_map) ;
					pool.submit(aug_thread) ;
					break ;
				}else{
					try {
						Thread.sleep(60*1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			break ;
		}
		pool.shutdown(); 
	}
	

	class AugmentThread implements Runnable , Serializable{

		private static final long serialVersionUID = 1L;

		private LinkedBlockingQueue<AugmentTableThreadBean> table_data_queue ;
		private String target_path ;
		private String error_path ;
		private String other_path ;
		private String source_save ;
		Map<String, IncrementDate> incre_date_map ;
		Map<String, Integer> bak_day_map ;
		Map<String, IncrDetailLog> incr_detail_map ;
		
		public AugmentThread() {
			super();
		}

		public AugmentThread(
				LinkedBlockingQueue<AugmentTableThreadBean> table_data_queue,
				String target_path, String error_path,
				String other_path, String source_save,
				Map<String, IncrementDate> incre_date_map,
				Map<String, Integer> bak_day_map,
				Map<String, IncrDetailLog> incr_detail_map) {
			super();
			this.table_data_queue = table_data_queue;
			this.target_path = target_path;
			this.error_path = error_path;
			this.other_path = other_path;
			this.source_save = source_save;
			this.incre_date_map = incre_date_map;
			this.bak_day_map = bak_day_map;
			this.incr_detail_map = incr_detail_map ;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			HiveConnection2 hive_conn = new HiveConnection2() ;
			FileListUtil file_util = new FileListUtil();
			HdfsFileSystem hfs = new HdfsFileSystem() ;
			AugmentTableThreadBean attb = null ;
			AugmentInfo aug_info = null ;
			List<SourceFileBean> source_bean_list ;
			String file_date = null ;
			String cim_job_name = null ;
			IncrementDate incre_date = null ;
			IncrDetailLog idil = null ;
			
			IncrDetailLogDao idildao = new IncrDetailLogDao() ;
			IncrementDateDao incre_date_dao = new IncrementDateDao() ;
			DataLoadBakTable dlbt = new DataLoadBakTable() ;
			HbaseConnection hbase_conn = new HbaseConnection() ;
			String bean_file_date = "" ;
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") ;
			while(true){
				// [[ a table ]]
				attb = table_data_queue.poll();
				System.out.println("剩余数据量：" + table_data_queue.size());
				if(attb instanceof AugmentTableThreadBean){
					// [[ a job ]]
					for(AugmentThreadBean atb : attb.getList()){
						/*
						 * atb -> aug_info -> table
						 */
						if( atb instanceof AugmentThreadBean){
							cim_job_name = atb.getCim_job_name() ;
							idil = incr_detail_map.get(cim_job_name) ;
							String begin_time = sdf.format(new Date()) ;
							aug_info = atb.getAug_info() ;
							source_bean_list = atb.getSource_bean_list() ;
							bean_file_date = source_bean_list.get(0).getFile_date() ;
							if(aug_info instanceof AugmentInfo){
								AugmentLog aug_log = new AugmentLog() ;
								aug_log.setAugment_id(Integer.parseInt(aug_info.getAug_id()));
								aug_log.setBegin_time(new Timestamp(System.currentTimeMillis()));
								boolean where = true ;
								String table_type = aug_info.getTable_type() ;
								String source_target_table_name = "" ;
								String target_table_name = "" ;
								int source_rownum = 0 ;
								int status = 0 ;
								try {
									/***********
									 * [ 配置表 ]
									 * 通过fs.write上传本地数据到hdfs
									 */
									if("配置表".equals(table_type)){
										source_target_table_name = aug_info.getTable_name() + " " ;
										target_table_name = aug_info.getTable_name() ;
										dlbt.data_load(bak_day_map , target_table_name , cim_job_name , source_bean_list, hfs, hive_conn) ;
										//【配置表在加载前会对文件日期进行比对，如果新下发文件日期在已经加载文件之前
										// 则不进行加载】
										file_date = ConfigTable.config_table_proces(aug_info, source_bean_list ,hive_conn , hfs , incre_date_map.get(cim_job_name)) ;
									}else if("明细表".equals(table_type) || "汇总表".equals(table_type)){
										if("1".equals(aug_info.getIs_partition())){
											source_target_table_name = aug_info.getMid_table_name() + " " ;
										}else{
											source_target_table_name = aug_info.getTable_name() + " " ;
										}
										target_table_name = aug_info.getTable_name() ;
										dlbt.data_load(bak_day_map , target_table_name , cim_job_name , source_bean_list, hfs, hive_conn) ;
										/*********
										 *[ 明细表和汇总表 ]
										 *[分区表    ] - 直接先os.write到临时表，再从临时表[insert into table ... select * from 临时表]
										 *[非分区表] - 直接os.write到目标表   
										 */
										file_date = DetailSummaryTable.detail_summary_table_proces(aug_info, source_bean_list ,hive_conn,hfs) ;
									}else if("信息表".equals(table_type)){
										/*********
										 * [ 信息表 ] 
										 * [ merge ] 实 现
										 */
										source_target_table_name = aug_info.getMid_table_name() + " " ;
										target_table_name = aug_info.getTable_name() ;
										dlbt.data_load(bak_day_map , target_table_name , cim_job_name , source_bean_list, hfs, hive_conn) ;
										file_date = InfoTable.config_table_proces(aug_info, source_bean_list ,hive_conn , hfs) ;
									}
								} catch (Exception e) {
									where = false ;
									System.err.println("error_table:" + target_table_name + "\t" + source_target_table_name);
									idil.setMark(idil.getMark() + "\n"
											+ "error_table:"
											+ target_table_name + "\t"
											+ source_target_table_name + "\n"
											+ e.getMessage());
									System.out.println(e.getMessage());
								}

								if(where && (!"".equals(file_date))){

									incre_date = incre_date_map.get(cim_job_name) ;
									if( incre_date instanceof IncrementDate ){
										if(file_date.compareTo(incre_date.getFile_date()) > 0){
											incre_date.setFile_date(file_date) ;
										}
										incre_date.setCim_job_name(cim_job_name) ;
										incre_date.setLoad_date(new Timestamp(System.currentTimeMillis())) ;
										incre_date_dao.update_increment_date(incre_date) ;
									}else{
										incre_date = new IncrementDate(0, target_table_name,cim_job_name, new Timestamp(System.currentTimeMillis()), file_date) ;
										incre_date_dao.insert_increment_date(incre_date) ;
									}
									hbase_conn.putDownloaded(target_table_name, file_date , cim_job_name ) ;
								}
								
								ResultSet rs = null ;

								/*文件正常导入
								 */
								if(where){
									idil.setSdatastatus("0") ;
									idil.setPdatastatus("0") ;
									if(!"".equals(file_date)){
										try {
											rs = hive_conn.execute2(" select count(*) from " + source_target_table_name) ;
											while(rs.next()){
												source_rownum = rs.getInt(1) ;
											}
											status = 0 ;
											rs.close() ;
										} catch (SQLException e) {
											System.err.println(e.getMessage());
										} 
									}else{
										idil.setPdatastatus("2") ;
									}
									file_util.create_directory(source_save + "/" + bean_file_date) ;
									file_util.file_mv3(source_bean_list, source_save + "/" + bean_file_date) ;
								}
								/*
								 * 文件处理出错处理
								 */
								else{
									idil.setSdatastatus("1") ;
									idil.setPdatastatus("1") ;
									source_rownum = 0 ;
									status = 1 ;
									file_util.create_directory(error_path + "/" + bean_file_date) ;
									file_util.file_mv3(source_bean_list, error_path + "/" + bean_file_date) ;
								}
								aug_log.setEnd_time(new Timestamp(System.currentTimeMillis()));
//								file_util.create_directory(source_save + "/" + bean_file_date) ;
								aug_log.setSource_path(file_util.list_to_string(source_bean_list, source_save + "/" + bean_file_date));
								aug_log.setSource_rownum(source_rownum);
								aug_log.setStatus(status); //0:正常  1:非正常
								AugmentLogDao.insert_aug_log(aug_log);
							}
							/*
							 *表不存在情况 
							 */
							else{
								file_util.create_directory(other_path + "/" + bean_file_date) ;
								file_util.file_mv3(source_bean_list, other_path + "/" + bean_file_date) ;
							}
							idil.setBegintime(begin_time) ;
							idil.setEndtime(sdf.format(new Date())) ;
							try {
								idildao.insert_incr_detail_log(idil) ;
							} catch (Exception e) {
								// TODO Auto-generated catch block
//								e.printStackTrace();
								System.out.println(e.getMessage());
							}
						}else{
							continue ;
						}
					}
				}else{
					break ;
				}
			}
			hbase_conn.putAll() ;
			incre_date_dao.close_conn(); 
			hive_conn.close(); 
			hfs.colse(); 
		}
		

		public String getTarget_path() {
			return target_path;
		}
		public void setTarget_path(String target_path) {
			this.target_path = target_path;
		}
		public String getError_path() {
			return error_path;
		}
		public void setError_path(String error_path) {
			this.error_path = error_path;
		}
		public String getOther_path() {
			return other_path;
		}
		public void setOther_path(String other_path) {
			this.other_path = other_path;
		}
		public String getSource_save() {
			return source_save;
		}
		public void setSource_save(String source_save) {
			this.source_save = source_save;
		}
		public LinkedBlockingQueue<AugmentTableThreadBean> getTable_data_queue() {
			return table_data_queue;
		}
		public void setTable_data_queue(
				LinkedBlockingQueue<AugmentTableThreadBean> table_data_queue) {
			this.table_data_queue = table_data_queue;
		}
		public Map<String, IncrementDate> getIncre_date_map() {
			return incre_date_map;
		}
		public void setIncre_date_map(Map<String, IncrementDate> incre_date_map) {
			this.incre_date_map = incre_date_map;
		}
		public Map<String, Integer> getBak_day_map() {
			return bak_day_map;
		}
		public void setBak_day_map(Map<String, Integer> bak_day_map) {
			this.bak_day_map = bak_day_map;
		}
		public Map<String, IncrDetailLog> getIncr_detail_map() {
			return incr_detail_map;
		}
		public void setIncr_detail_map(Map<String, IncrDetailLog> incr_detail_map) {
			this.incr_detail_map = incr_detail_map;
		}
	}
}
