package com.hiwan.dimp.service;

import java.io.Serializable;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.hiwan.dimp.bean.MataStockLog;
import com.hiwan.dimp.bean.StockMetaInfo;
import com.hiwan.dimp.dao.PageTool;
import com.hiwan.dimp.dao.StockMetaInfoDAO;
import com.hiwan.dimp.db.SqoopImport;
import com.hiwan.dimp.hive.HiveConnection;
/**
 * 全量数据导入脚本
 * @author terry
 *
 */
public class MainImport{
	public static Map<String, Integer> thread_map=new ConcurrentHashMap<String, Integer>() ;
	
	public static  void main(String[] args) throws Exception {
		
		Map<String,String> argss=new HashMap<String, String>();
		Options options=new Options();
		options.addOption("h","help", false, "Example:java -cp /root/dimp.jar com.hiwan.dimp.service.MainImport -h"); 
    	options.addOption("n","tableName", true, "TableName(String)");
    	options.addOption("t","tableType", true, "1:xinxi,2:peizhi,3:mingxi,4:huizong. You must be use a comma separated");
    	options.addOption("pw","password", true, "if -t must input -pw password");
    	options.addOption("i","init", true, "Init hive");


    	//以“-”开头的单个字符的POSIX风格；以“--”后接选项关键字的GNU风格。 
    	CommandLineParser parser = new PosixParser(); 
    	CommandLine cmd = parser.parse(options, args);
    	HelpFormatter hf = new HelpFormatter();
    	if(args==null || args.length<1)
		{		
			hf.printHelp("MainImport", options,true);
			System.exit(0);
		}
    	
    	argss.put("status","0");
    	argss.put("mr","70");//设定map总数
    	argss.put("splitCount","140000");//block转化map分割数
    	
    	
    	 if(cmd.hasOption("h")) { 
	    		hf.printHelp("MainImport", options,true);
	    }else 	if(cmd.hasOption("n")) { 
	    		String tableName=cmd.getOptionValue("n");
	    		argss.put("tableName",tableName);
	    		new MainImport().impStockData(argss); 
	    	}else if (cmd.hasOption("t")) {
	    		String password=cmd.getOptionValue("pw");
				if(password!=null && password.equals("zdq"))
				{
		    		String tableType = cmd.getOptionValue("t");
					//System.out.println(tableType);
					String[] t=tableType.split(",");
					List<String> list=Arrays.asList(t);
					if(list.contains("1")){
						argss.put("xinxi", "信息表");
					}
					if(list.contains("2")){
						argss.put("peizhi", "配置表");
					}
					if(list.contains("3")){
						argss.put("mingxi", "明细表");				
					}
					if(list.contains("4")){
						argss.put("huizong","汇总表");
					}
					new MainImport().impStockData(argss); 
				}else
				{
					System.out.println("password wrong!!");
				}
			}if(cmd.hasOption("i")) { //初始化
				String password=cmd.getOptionValue("pw");
				if(password!=null && password.equals("zdq"))
				{
	    		String beginRow=cmd.getOptionValue("i");
	    		argss.clear();
	    		argss.put("beginRow", beginRow);
	    		if (cmd.hasOption("t")) {
					String tableType = cmd.getOptionValue("t");
					//System.out.println(tableType);
					String[] t=tableType.split(",");
					List<String> list=Arrays.asList(t);
					if(list.contains("1")){
						argss.put("xinxi", "信息表");
					}
					if(list.contains("2")){
						argss.put("peizhi", "配置表");
					}
					if(list.contains("3")){
						argss.put("mingxi", "明细表");				
					}
					if(list.contains("4")){
						argss.put("huizong","汇总表");
					}
				}
	    		MataCreateTable.update(argss);
				}
	    		
	    	}else
			{
				hf.printHelp("MainImport", options,true);
			}
	    		
		 
	}
	
	public static  void mainA(String[] args) throws Exception {
	
		Map<String,String> argss=new HashMap<String, String>();
		Options options=new Options();
		options.addOption("h","help", false, "Example:java -cp /root/dimp.jar com.hiwan.dimp.service.MainImport -h"); 
    	options.addOption("i","init", true, "Init hive");
    	options.addOption("s","status", true, "Tb_table_metadata status(Integer) default 0"); 
    	options.addOption("n","tableName", true, "TableName(String)");
    	options.addOption("t","tableType", true, "1:xinxi,2:peizhi,3:mingxi,4:huizong. You must be use a comma separated");
    	options.addOption("m","mapreduce", true, "Table mapreduce num(Integer) default 50");
    	options.addOption("sc","splitCount", true, "Table splitCount num(Integer) default 140000");
    	//以“-”开头的单个字符的POSIX风格；以“--”后接选项关键字的GNU风格。 
    	CommandLineParser parser = new PosixParser(); 
    	CommandLine cmd = parser.parse(options, args);
    	HelpFormatter hf = new HelpFormatter();
    	if(args==null || args.length<1)
		{		
			hf.printHelp("MainImport", options,true);
			System.exit(0);
		}
    	
    	argss.put("status","0");
    	argss.put("mr","50");//设定map总数
    	argss.put("splitCount","140000");//block转化map分割数
    	
    	
    	if(cmd.hasOption("i")) { //初始化
    		String beginRow=cmd.getOptionValue("i");
    		argss.clear();
    		argss.put("beginRow", beginRow);
    		if (cmd.hasOption("t")) {
				String tableType = cmd.getOptionValue("t");
				//System.out.println(tableType);
				String[] t=tableType.split(",");
				List<String> list=Arrays.asList(t);
				if(list.contains("1")){
					argss.put("xinxi", "信息表");
				}
				if(list.contains("2")){
					argss.put("peizhi", "配置表");
				}
				if(list.contains("3")){
					argss.put("mingxi", "明细表");				
				}
				if(list.contains("4")){
					argss.put("huizong","汇总表");
				}
			}
    		MataCreateTable.update(argss);
    		
    	}else if(cmd.hasOption("h")) { 
	    		hf.printHelp("MainImport", options,true);
	    }else{
	    	if(cmd.hasOption("s")) { 
	    		String status=cmd.getOptionValue("s");
	    		argss.put("status",status==null?"0":status);
	    	}
	    	if(cmd.hasOption("m")) { 
	    		String mapreduce=cmd.getOptionValue("m");
	    		argss.put("mr",mapreduce==null?"50":mapreduce );
	    	}
	    	if(cmd.hasOption("sc")) { 
	    		String splitCount=cmd.getOptionValue("sc");
	    		argss.put("splitCount",splitCount==null?"140000":splitCount);
	    	}
	    	if(cmd.hasOption("n")) { 
	    		String tableName=cmd.getOptionValue("n");
	    		argss.put("tableName",tableName);
	    	}
	    	if (cmd.hasOption("t")) {
				String tableType = cmd.getOptionValue("t");
				//System.out.println(tableType);
				String[] t=tableType.split(",");
				List<String> list=Arrays.asList(t);
				if(list.contains("1")){
					argss.put("xinxi", "信息表");
				}
				if(list.contains("2")){
					argss.put("peizhi", "配置表");
				}
				if(list.contains("3")){
					argss.put("mingxi", "明细表");				
				}
				if(list.contains("4")){
					argss.put("huizong","汇总表");
				}
			}
	    	//argss.put("peizhi", "配置表");
	    	new MainImport().impStockData(argss); 
	    }
	}
	/**
	 * 存量数据的导入
	 * @throws Exception
	 */
	public void impStockData(Map<String,String> commandParameter) throws Exception {
		StockMetaInfoDAO dao = new StockMetaInfoDAO();
		//Map<String, String> map = new HashMap<String, String>();
		//map.put("xinxi", "信息表");
		//map.put("peizhi", "配置表");
		//map.put("mingxi", "明细表");
		//map.put("huizong","汇总表");
		//map.put("is_partition", "0");
		// 定义总mr数量
		int mrCount = Integer.parseInt(commandParameter.get("mr"));
		// 定义分割的数量
		int splitCount = Integer.parseInt(commandParameter.get("splitCount"));
		//map.put("status", commandParameter.get("status"));
		//获取需要导入的表
		List<StockMetaInfo> list = dao.getInfoList(commandParameter);
		
		System.out.println("共有"+list.size()+"张表将要执行。");
		//预先定义线程
		MetaImpThread metaImpThread = null;
		//线程池
		//ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(20);
		ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newCachedThreadPool();
		//当前sqoop中导入的表名
		String	mr_table_name = null;
		//累计mr数量
		int mr_cur_num = 0 ;
		//循环matedata
		
		for (StockMetaInfo mt : list) {
			System.out.println("Current tableName:"+mt.getTable_name());
			//如果表的Oracle不存在
			if(!PageTool.isExist(mt.getTable_name())){
					mt.setStatus("4");
					StockMetaInfoDAO.updateMateStatus(mt);
					System.out.println(mt.getTable_name()+"表在Oracle不存在，将其状态置为4");
					continue;
			}
			/*Map<String, String> pm = new HashMap<String, String>();
			pm.put("tablename", mt.getTable_name());
			pm.put("owner", "CPDDS_PDATA");
			Integer blCount = dao.blockCount(pm);
			
			//第张表 分割后需要的mr数量			
			//分割后需要的mr数量
			int mapCount =  0;	
			int modCount = blCount % splitCount;	
			
			if(modCount==0){
				mapCount =  blCount / splitCount;	
			}else{
				mapCount = (blCount / splitCount) + 1;
			}*/
			mr_table_name = mt.getTable_name();
			HiveConnection hc = new HiveConnection();	
			//如果存在先drop在创建					
			String dropTable = " drop table if exists " + mt.getTable_name().toUpperCase();
			hc.execute(dropTable);
			String hive = mt.getCreate_script_hive();
			ResultSet rs = hc.execute(hive);
			rs.close();
			hc.close();
			
			//创建分区脚本 、执行导入分区数据
			if (mt.getIs_partition().equals("1")) {// '1是分区表，0为不是分区',				
				List<StockMetaInfo> sp = mt.getList();
				//循环分区
				for(StockMetaInfo sc:sp){
					System.out.println("mt.getTable_name()"+mt.getTable_name());
					
					MataStockLog  msl=new MataStockLog();
					
					StockMetaInfo aloneSmi=mt.clone();
					//参数Map
					Map<String, String> paramMap = new HashMap<String, String>();
					paramMap.put("tableName", mt.getTable_name());
					paramMap.put("primaryKey", mt.getPrimary_key());
					paramMap.put("tableType",mt.getTable_type());
					
					if(sc.getAdd_partition_script()!=null){	
						System.out.println("mt.getTable_name()1"+mt.getTable_name());			
						Map<String, String> pm = new HashMap<String, String>();
						pm.put("tablename", mt.getTable_name());
						pm.put("owner", "CPDDS_PDATA");
						pm.put("partition_value", sc.getPartition_value());
						Integer blCount = dao.blockCount(pm);
						//第张表 分割后需要的mr数量			
						//分割后需要的mr数量
						int mapCount =  0;	
						int modCount = blCount % splitCount;	
						if(modCount==0){
							mapCount =  blCount / splitCount;	
						}else{
							mapCount = (blCount / splitCount) + 1;
						}
						
						HiveConnection hiveconn = new HiveConnection();	
						//创建hive分区
						String partition_script=sc.getAdd_partition_script();
						//分区ID
						String partition_id=sc.getSt_id();
						
						ResultSet rs2 = hiveconn.execute(partition_script);
						rs2.close();
						hiveconn.close();
						String partition_value=sc.getPartition_value();
						Iterator<Map.Entry<String, Integer>> it = thread_map.entrySet().iterator();
						while (it.hasNext()) {
							Map.Entry<String, Integer> entry1 = it.next();
							mr_cur_num += entry1.getValue();
						}
						while (mrCount < mr_cur_num + mapCount) {
							if (pool.getPoolSize() > 0) {
								int count_num = 0;
								Iterator<Map.Entry<String, Integer>> it1 = thread_map.entrySet().iterator();
								while (it1.hasNext()) {
									Map.Entry<String, Integer> entry1 = it1.next();
									count_num += entry1.getValue();
								}
								if (mrCount > count_num + mapCount) {
									mr_cur_num = count_num;
									break;
								}
								if(thread_map.isEmpty()){
									break;
								}
							} else {
								break;
							}
						}
						//每行sqoop命令导入需要的map数量
						paramMap.put("num", mapCount+"");
						//记录当前sqoop命令线程需要的map数
						thread_map.put(mt.getTable_name()+partition_value, mapCount) ;
						paramMap.put("is_partition","1");
						paramMap.put("partition_name","p_date");
						paramMap.put("partition_field",sc.getPartition_field());
						paramMap.put("partition_value",partition_value);													
						// 针对分区和非分区进行特殊处理
						msl.setPartion_id(partition_id);	
						System.out.println("传入mt参数:"+aloneSmi);
						aloneSmi.setPartition_value(partition_value);
						//aloneSmi.setSt_status("0");
						metaImpThread =new MetaImpThread(paramMap,aloneSmi,msl);
						// 执行
						pool.submit(metaImpThread) ;
					}
				}
			}else{//非分区
				MataStockLog  msl=new MataStockLog();
				//参数Map
				Map<String, String> paramMap = new HashMap<String, String>();
				paramMap.put("tableName", mt.getTable_name());
				paramMap.put("primaryKey", mt.getPrimary_key());
				paramMap.put("tableType",mt.getTable_type());
				
				Map<String, String> pm = new HashMap<String, String>();
				pm.put("tablename", mt.getTable_name());
				pm.put("owner", "CPDDS_PDATA");
				Integer blCount = dao.blockCount(pm);
				//第张表 分割后需要的mr数量			
				//分割后需要的mr数量
				int mapCount =  0;	
				int modCount = blCount % splitCount;	
				if(modCount==0){
					mapCount =  blCount / splitCount;	
				}else{
					mapCount = (blCount / splitCount) + 1;
				}				
				Iterator<Map.Entry<String, Integer>> it = thread_map.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry<String, Integer> entry1 = it.next();
					mr_cur_num += entry1.getValue();
				}
				while (mrCount < mr_cur_num + mapCount) {
					if (pool.getPoolSize() > 0) {
						int count_num = 0;
						Iterator<Map.Entry<String, Integer>> it1 = thread_map.entrySet().iterator();
						while (it1.hasNext()) {
							Map.Entry<String, Integer> entry1 = it1.next();
							count_num += entry1.getValue();
						}
						if (mrCount > count_num + mapCount) {
							mr_cur_num = count_num;
							break;
						}
						if(thread_map.isEmpty()){
							break;
						}
					} else {
						break;
					}
				}
				//每行sqoop命令导入需要的map数量
				paramMap.put("num", mapCount+"");
				//记录当前sqoop命令线程需要的map数
				thread_map.put(mr_table_name, mapCount) ;
				paramMap.put("partition_name",null);
				paramMap.put("is_partition","0");
				metaImpThread =  new MetaImpThread(paramMap,mt,msl);
				//执行
				pool.submit(metaImpThread) ;
				//每次执行后赋值
			}
		}
		// 不在往线程池中添加线程
		pool.shutdown();	
	}
  class MetaImpThread implements Runnable,Serializable{
	  	private static final long serialVersionUID = 1L;
		private Map<String, String> paramMap;
	    private StockMetaInfo mt;
	    private MataStockLog msl;
		public MetaImpThread(Map<String, String> paramMap,StockMetaInfo mt,MataStockLog msl){
			this.paramMap=paramMap;
			this.mt=mt;
			this.msl=msl;
		}
		@Override
		public void run() {
			msl.setBegin_time(System.currentTimeMillis() + "");
			// 启动线程导入数据
			int flag = SqoopImport.impTableData(paramMap);
			System.out.println("Sqoop导入成功标志:"+flag);
			// 校验数据
			Iterator<Entry<String, Integer>> it = MainImport.thread_map.entrySet().iterator();
			while (it.hasNext()) {
				Entry<String, Integer> entry = it.next();
				String key = entry.getKey();
				if (paramMap.get("is_partition")!=null&&paramMap.get("is_partition").equals("1")) {
					System.out.println("当前表是分区表,表名是："+paramMap.get("tableName")+",分区是："+paramMap.get("partition_value"));
					if (key.equals(paramMap.get("tableName")+ paramMap.get("partition_value"))) {
						it.remove();
						System.out.println("分区释放了Map数");
					}
					//更新tb_table_stock表，表示表的某分区数据已经导入
					mt.setSt_status(flag+"");
					//查询表所有分区的状态是不是都成功，成功则将表的状态改变
//					if(StockMetaInfoDAO.queryTablePartionStatus(mt)){
//						mt.setStatus("1");
//					}
				} else {
					if (key.equals(paramMap.get("tableName"))) {
						System.out.println("当前表是非分区表,表名是："+paramMap.get("tableName"));
						it.remove();
						System.out.println("表释放了Map数");
					}
					mt.setStatus(flag+"");
				}
			}
			
			//   TODO: 测试程序时验证导入数据是否正确.上正式环境  source_rownum ,target_rownum 以下四行需要注释掉
			//int source_rownum = Integer.parseInt(mt.getRow_num());
			//int target_rownum = new HiveUtils().rowCount(mt.getTable_name());
			//msl.setSource_rownum(source_rownum+"");// 源数据条数
			//msl.setTarget_rownum(target_rownum+"");// 目标数据条数
			  
			//  记录完成信息,更新matedata状态
			msl.setEnd_time(System.currentTimeMillis() + "");
			//System.out.println("执行logInfo()函数");
			//System.out.println("mt>>>>>>"+mt.toString());
			//System.out.println("msl>>>>>>"+msl.toString());
			logInfo(mt, msl);
			System.out.println("当前线程执行结束:"+paramMap.get("tableName")+"  "+paramMap.get("partition_value"));
		}
		
		public  long getSerialversionuid() {
			return serialVersionUID;
		}
		public Map<String, String> getParamMap() {
			return paramMap;
		}
		public void setParamMap(Map<String, String> paramMap) {
			this.paramMap = paramMap;
		}
		/**
		 * 第5步骤
		 * 记录信息,更新matedata脚本状态
		 * 向Tb_table_stock_log 写入记录信息
		 * @throws Exception 
		 */
		public void logInfo(StockMetaInfo smi,MataStockLog msl){
			//--- wxy-----
			System.out.println("插入时msl："+msl);
			System.out.println("插入时smi："+smi);
			
			//    更新tb_table_stock status信息，1表示已经执行加载过分区数据
			//是分区，且
			if(smi.getIs_partition().equals("1")&&smi.getStatus().equals("0")){
				System.out.println("执行分区状态的更新");
				StockMetaInfoDAO.updateStockStatus(smi);
				
				if(StockMetaInfoDAO.queryTablePartionStatus(smi)){
					smi.setStatus("1");
				}
			}
			/*if(smi.getIs_partition().equals("1")&&smi.getStatus().equals("1")){
				StockMetaInfoDAO.updateMateStatus(smi);
			}*/
			if(smi.getStatus().equals("1")){
				StockMetaInfoDAO.updateMateStatus(smi);
			}
			StockMetaInfoDAO.insertLogInfo(smi, msl);
			
		}
		
	}
  	
	/**
	 * 1、提供帮助服务  java -cp dimp.jar com.hiwan.dimp.service.MainImport --help
	 * 2、提供各个参数功能解释
	 */
	public static void usage(){
		System.out.println();
		System.out.println();
		System.out.println();
	}
}
