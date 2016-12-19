package com.hiwan.dimp.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.hiwan.dimp.bean.StockMetaInfo;
import com.hiwan.dimp.dao.StockMetaInfoDAO;
import com.hiwan.dimp.db.IncrementImport;
import com.hiwan.dimp.service.MainImport.MetaImpThread;
/**
 * 增量数据主控程序
 * @author tx
 *
 */
public class MainImportAdd {
	public static void main(String[] args) throws Exception {
		new MainImportAdd().impAddData(args); 
	}
	
	/**
	 * 增量数据导入
	 * @param args
	 * @throws Exception 
	 */
	private void impAddData(String[] args) throws Exception {
		StockMetaInfoDAO dao = new StockMetaInfoDAO();
		Map<String, String> map = new HashMap<String, String>();
		//map.put("xinxi", "信息表");
		//map.put("peizhi", "配置表");
		//map.put("mingxi", "明细表");
		//map.put("huizong","汇总表");
		//map.put("status", "0");
		
		//预先定义线程
		MetaImpThread metaImpThread = null;
		//线程池
		//ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(20);
		ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newCachedThreadPool();
		
		//TODO  假设 遍历本地文件系统中有增量数据的表  的结果集
		List<String> list=new ArrayList<String>();
		//  FIXME
		for (String tableName : list) {
			StockMetaInfo smi=null;
			map.put("tableName", tableName);
			List<StockMetaInfo> tableInfo = dao.getInfoList(map);
			if(tableInfo!=null&&tableInfo.size()==1){
				smi=tableInfo.get(0);
			}
			//TODO   要添加的属性
			map.put("localPath", "");
			map.put("bucket", smi.getBucket());
			
			
			
		}
	}
	class AugmentThread implements Runnable{
		private Map<String,String> map;
		public AugmentThread(Map<String,String> map) {
			this.map=map;
		}
		@Override
		public void run() {
			IncrementImport.impTableData(map);
		}
		
	}
	
}
