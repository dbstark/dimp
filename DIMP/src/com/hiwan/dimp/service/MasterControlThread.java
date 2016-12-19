package com.hiwan.dimp.service;

import java.io.Serializable;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.hiwan.dimp.bean.MataStockLog;
import com.hiwan.dimp.bean.StockMetaInfo;
import com.hiwan.dimp.dao.StockMetaInfoDAO;
import com.hiwan.dimp.db.SqoopImport;
import com.hiwan.dimp.hbase.HbaseUtils;
import com.hiwan.dimp.hive.HiveConnection;
import com.hiwan.dimp.hive.HiveUtils;
import com.hiwan.dimp.service.MasterControlProgramThread.MetaImpThread;
import com.hiwan.dimp.test.MyThread;
import com.hiwan.dimp.test.ThreadPoolTest;
/**
 * 存量数据导入脚本
 * @author terry
 *
 */
public class MasterControlThread {
	public static Map<String, Integer> threadMap=new ConcurrentHashMap<String, Integer>() ;
	
	/**
	 *  	1---创建表
	 *  	2---add_partition
	 *  	3---导数据
	 *  	4---核实数据
	 *  	5---记录信息
	 */
	public static  void main(String[] args) throws Exception {
		//TODO: 第一个代表开启线程数.第二个开启总 mr数量.第三个是设置blocks分割参数
		args = new String[]{"10","20","30"} ;
		new MasterControlThread().impStockData(args); 
		
	}
	/**
	 * 第5步骤
	 * 记录信息,更新matedata脚本状态
	 * 向Tb_table_stock_log 写入记录信息
	 * @throws Exception 
	 */
	public void logInfo(StockMetaInfo smi,MataStockLog msl) throws Exception{
		StockMetaInfoDAO.updateMateStatus(smi);
		StockMetaInfoDAO.insertLogInfo(smi, msl);
	}

	/**
	 * 存量数据的导入
	 * @throws Exception
	 */
	public void impStockData(String[] args) throws Exception {
		StockMetaInfoDAO dao = new StockMetaInfoDAO();
		Map<String, String> map = new HashMap<String, String>();
		map.put("xinxi", "信息表");
		map.put("peizhi", "配置表");
		map.put("status", "4");
		// 定义线程数
		int thCount = Integer.valueOf(args[0]);
		// 定义总mr数量
		int mrCount = Integer.valueOf(args[1]);
		// 定义分割的数量
		int splitCount = Integer.valueOf(args[2]);
		 	
		List<StockMetaInfo> list = dao.getInfoList(map);		 
		//预先定义线程
		MetaImpThread metaImpThread = null;
		//线程池
		//ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newCachedThreadPool();	 
		ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(thCount);
		//当前运行线程需要mr数量
		int mr_num = 0 ;
		//当前sqoop中导入的表名
		String	mr_table_name = null ;
		//当前线程累计mr数量
		int mr_cur_num = 0 ;
		for (StockMetaInfo mt : list) {
			Map<String, String> pm = new HashMap<String, String>();
			pm.put("tablename", mt.getTable_name());
			pm.put("owner", "CPDDS_PDATA");
			//查询块数量
			Integer blCount = dao.blockCount(pm);			
			//分割后需要的mr数量
			int mapCount =  0;	
			int modCount = blCount % splitCount;	
			
			if(modCount==0){
				mapCount =  blCount / splitCount;	
			}else{
				mapCount = (blCount / splitCount) + 1;
			}					
			//当前线程需要的mr数量
			mr_num = mapCount;
			//当前线程中导入的表名称
			mr_table_name = mt.getTable_name() ;
			//已有线程累加mr数量
			//mr_cur_num += mapCount ;	
			Map<String, Integer> tt = Collections.synchronizedMap(threadMap);
			Iterator<Map.Entry<String, Integer>> it = tt.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, Integer> entry2 = it.next();
				mr_cur_num += entry2.getValue();
			}		
			//累计数大于设定数
			while (mrCount < mr_cur_num + mr_num) {
				// 返回活动线程数量
				if (pool.getPoolSize() > 0) {
					int count_num = 0;
					// 获取活动线程中mr的累加数量
					Map<String, Integer> hh = Collections.synchronizedMap(threadMap);
					Iterator<Map.Entry<String, Integer>> it1 = hh.entrySet().iterator();
					while (it1.hasNext()) {
						Map.Entry<String, Integer> entry = it1.next();
						count_num += entry.getValue();
					}
					if (mrCount > count_num + mr_num) {
						mr_cur_num = count_num;
						break;
					}
					Thread.sleep(1 * 1000);
					System.out.println("sleep");
				} else {
					break;
				}
			}				
		 
			//添加当前线程需要的mr数量
			threadMap.put(mr_table_name, mr_num);
			metaImpThread =  new MetaImpThread(mt, String.valueOf(mapCount));
			//执行
			pool.submit(metaImpThread) ;

		}
		// 不在往线程池中添加线程
		pool.shutdown();	
	}
  class MetaImpThread implements Runnable,Serializable{		 
		private  StockMetaInfo  mt;
		private static final long serialVersionUID = 1L;
		private String mapCount; 
		public MetaImpThread(StockMetaInfo mt,String mapCount){
		 
			this.mt = mt;
			this.mapCount = mapCount;	 
		}
		@Override
		public void run() {
				try{
					MataStockLog  msl=new MataStockLog();			
					msl.setBegin_time(System.currentTimeMillis()+"");
					//获取hbase
					String hbase = mt.getCreate_script_hbase();			
					String hive = mt.getCreate_script_hive();			
					 if(mt.getTable_type()!=null&&"信息表".equals(mt.getTable_type())){
						//只有信息表会到hbase  创建hbase脚本
						HbaseUtils.createTable(mt.getTable_name());
					}
					HiveConnection hc1 = new HiveConnection();	
					//创建hive 迁移过程中如果存在先drop在创建					
					String droptable = " drop table if exists " + mt.getTable_name().toUpperCase();
					ResultSet rs1 = hc1.execute(droptable);
					rs1.close();
					hc1.close();
										
					HiveConnection hc2 = new HiveConnection();
					ResultSet rs2 = hc2.execute(hive);
					rs2.close();
					hc2.close(); 
					//创建分区脚本
					if (mt.getIs_partition().equals("1")) {// '1是分区表，0为不是分区',				
						List<StockMetaInfo> sp = mt.getList();
						for(StockMetaInfo sc:sp){
							if(sc.getAdd_partition_script()!=null){						
								 HiveConnection hiveconn = new HiveConnection();	
								//创建hive
								ResultSet rs3 = hiveconn.execute(sc.getAdd_partition_script());
								rs3.close();
								hiveconn.close(); 
							}						
						}								
					}						
					Map<String, String> impMap = new HashMap<String, String>();
					impMap.put("tableName", mt.getTable_name());
					impMap.put("primaryKey", mt.getPrimary_key());
					impMap.put("num", mapCount);
					impMap.put("tableType",mt.getTable_type());
					//导入数据 
					SqoopImport.impTableData(impMap);
					//校验数据			
					int source_rownum=Integer.parseInt(mt.getRow_num());
					int target_rownum= new HiveUtils().rowCount(mt.getTable_name());			
					msl.setSource_rownum(source_rownum+"");//源数据条数
					msl.setTarget_rownum(target_rownum+"");//目标数据条数
					int status=(source_rownum!=target_rownum)?0:1;
					msl.setStatus(status+"");			
					msl.setEnd_time(System.currentTimeMillis()+"");			
					//记录完成信息,更新matedata状态
					mt.setStatus("1");
					logInfo(mt,msl);											 
			}catch(Exception e){
				
			}				
			Iterator<Entry<String, Integer>> it = ThreadPoolTest.thread_map.entrySet().iterator();
			while (it.hasNext()) {
				Entry<String, Integer> entry = it.next();
				String key = entry.getKey();
					if (key.equals(mt.getTable_name())) {
						it.remove();
					}
				}			
		      System.out.println("MasterControlThread.threadMap:"+MasterControlThread.threadMap);
		}
		public StockMetaInfo getMt() {
			return mt;
		}
		public void setMt(StockMetaInfo mt) {
			this.mt = mt;
		}
		public String getMapCount() {
			return mapCount;
		}
		public void setMapCount(String mapCount) {
			this.mapCount = mapCount;
		}
		/*public Map<String, Integer> getThreadMap() {
			return threadMap;
		}
		public void setThreadMap(Map<String, Integer> threadMap) {
			this.threadMap = threadMap;
		}*/
		public  long getSerialversionuid() {
			return serialVersionUID;
		}
		
		
		
	}
}
