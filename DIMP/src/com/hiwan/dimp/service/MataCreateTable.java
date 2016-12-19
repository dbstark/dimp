package com.hiwan.dimp.service;

import java.util.List;
import java.util.Map;

import com.hiwan.dimp.bean.StockMetaInfo;
import com.hiwan.dimp.dao.PageTool;
import com.hiwan.dimp.dao.StockMetaInfoDAO;
/**
 * 生成建表脚本
 * @author terry
 *
 */
public class MataCreateTable {
	
	public static  void main(String[] args) throws Exception {
		 
		new MataCreateTable().updateStock();
		
	}
	/**
	 * 生成创建脚本
	 * @throws Exception
	 */
	public   void createTable() throws Exception {		
		List<String> tables=PageTool.allTables();
		System.out.println("Oracle中表的数目:"+tables.size());
		for (String tableName : tables) {
			StockMetaInfo smi=PageTool.getMetafo(tableName);
			smi=PageTool.insertMetaInfo(smi);
			PageTool.insertStockInfo(smi);
		}
		
		System.out.println("创建脚本完成");
			 
	}
	//@Test
	public void createTableTest() throws Exception{
		new MataCreateTable().createTable(); 
	}
	//更新hive和hbase
	public  static void update(Map<String,String> map) throws Exception{
		UpdateStockMetaInfo usmi= null;
		List<StockMetaInfo> tables=new  StockMetaInfoDAO().getInfoList(map);
		System.out.println(tables.size());
		/*System.out.println(tables.get(1));
		System.out.println(tables.get(98));*/
		System.out.println("初始化metadata表中所有信息表的Hive属性.....请等待");
		for (StockMetaInfo stockMetaInfo : tables) {
			//if(stockMetaInfo.getTable_type().equals("汇总表")||stockMetaInfo.getTable_type().equals("明细表")||stockMetaInfo.getTable_type().equals("信息表")){
			//if((stockMetaInfo.getTable_type().equals("配置表"))){
				usmi=new UpdateStockMetaInfo();
				usmi.updateHiveAndHbaseInfo(stockMetaInfo);
				//System.out.println(resultScript);
			//}//
		}
		System.out.println("初始化完成,请执行导入数据功能");
	}
	//更新`tb_table_stock`,添加分区
	//
	 public void updateStock() throws Exception{
		 UpdateStockMetaInfo usmi=null;
		 /* Map<String,String> map=new HashMap<String,String>();
		 map.put("tableName", tableName);*/
		 //要清空tb_table_stock
		 
		 List<StockMetaInfo> tables=new  StockMetaInfoDAO().getMetaInfoList(null);
		 
		 for (StockMetaInfo stockMetaInfo : tables) {
				System.out.println(stockMetaInfo.toString());
				usmi=new UpdateStockMetaInfo();
				usmi.updateStock(stockMetaInfo);
		}
		 
	 }
}
