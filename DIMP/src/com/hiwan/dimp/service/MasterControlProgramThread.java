package com.hiwan.dimp.service;

import java.io.Serializable;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.hiwan.dimp.bean.MataStockLog;
import com.hiwan.dimp.bean.StockMetaInfo;
import com.hiwan.dimp.dao.StockMetaInfoDAO;
import com.hiwan.dimp.db.SqoopImport;
import com.hiwan.dimp.hbase.HbaseUtils;
import com.hiwan.dimp.hive.HiveConnection;
import com.hiwan.dimp.hive.HiveUtils;
/**
 * 存量数据导入脚本
 * @author terry
 *
 */
public class MasterControlProgramThread {
	
	/**
	 *  	1---创建表
	 *  	2---add_partition
	 *  	3---导数据
	 *  	4---核实数据
	 *  	5---记录信息
	 */
	public static  void main(String[] args) throws Exception {
		//this.createHiveAndHbase();
		new MasterControlProgramThread().impStockData(); 
		
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
	public  void impStockData() throws Exception {		
		StockMetaInfoDAO dao = new StockMetaInfoDAO();
		Map<String, String> map = new HashMap<String, String>();
		map.put("xinxi", "信息表");
		map.put("peizhi", "配置表");
		map.put("status", "4");
		
		List<StockMetaInfo> list = dao.getInfoList(map);					
		System.out.println("list :"+list.size());
		Integer modelSize = list.size();//数据量	
		//500条数据一个线程
		int pnum=400;
		Integer threadCount = modelSize / pnum;
		Integer threadYu = modelSize % pnum;
		
		ThreadPoolExecutor  pool = (ThreadPoolExecutor) Executors.newCachedThreadPool();
		for (Integer  index = 0; index < threadCount + 1; index ++) {			  			 
			Thread impThread;			
			if(index==threadCount){
				impThread = new Thread(new MetaImpThread(list.subList(index * pnum, pnum *index + threadYu),map));
			}else{
				impThread = new Thread(new MetaImpThread(list.subList(index*pnum, pnum *(index+1)),map));
			} 		
			pool.submit(impThread);	
		}				
		//不在往线程池中添加线程
		 pool.shutdown();
		//总任务数
		long taskcount= pool.getTaskCount();
		//计算已经完成的线程
		long completedTaskCount = pool.getCompletedTaskCount();						
		while(completedTaskCount!=taskcount){
			  completedTaskCount = pool.getCompletedTaskCount();
			  taskcount= pool.getTaskCount();
		}			
	}
   class MetaImpThread implements Runnable,Serializable{
		 
		private List<StockMetaInfo> list;
		private static final long serialVersionUID = 1L;
		private Map<String,String> map; 
		public MetaImpThread(List<StockMetaInfo> list,Map<String,String> map){
		 
			this.list = list;
			this.map = map;
		 
		}
		@Override
		public void run() {
			try{				
				for (StockMetaInfo mt : list) {			
					MataStockLog  msl=new MataStockLog();			
					msl.setBegin_time(System.currentTimeMillis()+"");
					//获取hbase
					String hbase = mt.getCreate_script_hbase();			
					String hive = mt.getCreate_script_hive();			
					if(mt.getTable_type()!=null&&"信息表".equals(mt.getTable_type())){
						//只有信息表会到hbase  创建hbase脚本
						HbaseUtils.createTable(mt.getTable_name());
					}
					HiveConnection hc = new HiveConnection();	
					//创建hive 迁移过程中如果存在先drop在创建					
					String droptable = " drop table if exists " + mt.getTable_name().toUpperCase();
					hc.execute(droptable);
					
					ResultSet rs = hc.execute(hive);
					rs.close();
					hc.close();
					//创建分区脚本
					if (mt.getIs_partition().equals("1")) {// '1是分区表，0为不是分区',				
						List<StockMetaInfo> sp = mt.getList();
						for(StockMetaInfo sc:sp){
							if(sc.getAdd_partition_script()!=null){						
								HiveConnection hiveconn = new HiveConnection();	
								//创建hive
								ResultSet rs2 = hiveconn.execute(sc.getAdd_partition_script());
								rs2.close();
								hiveconn.close();
							}						
						}								
					}						
					Map<String, String> impMap = new HashMap<String, String>();
					impMap.put("tableName", mt.getTable_name());
					impMap.put("primaryKey", mt.getPrimary_key());
					impMap.put("num", "4");
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
				}
				
				 
			}catch(Exception e){
				
				 	}
		}
		
	}
}
