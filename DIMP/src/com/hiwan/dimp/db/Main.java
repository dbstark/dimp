package com.hiwan.dimp.db;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.hiwan.dimp.bean.MataStockLog;
import com.hiwan.dimp.bean.StockMetaInfo;
import com.hiwan.dimp.dao.PageTool;
import com.hiwan.dimp.dao.StockMetaInfoDAO;
import com.hiwan.dimp.hbase.HbaseUtils;
import com.hiwan.dimp.hive.HiveConnection;
import com.hiwan.dimp.hive.HiveUtils;

public class Main {
	//@Test
	public void hbaseCreateTable(){
		HbaseUtils.createTable("ACCINF");
	}
	/**
	 *  @Describe:hbase建表测试
	 *  @time  :2015年7月9日 上午10:18:51
	 */
	//@Test 
	public void hiveCreateTable(){
		HiveConnection hc=new HiveConnection();
		System.out.println(hc);
	}
	public static void main(String[] args) throws SQLException {
			StockMetaInfo smi=PageTool.getMetafo("AMRGT_ACCT_DATE");
			System.out.println(smi);
			//smi=PageTool.insertMetaInfo(smi);
			//System.out.println(smi.getTable_id());
			//PageTool.insertStockInfo(smi);
	}
	@Test
	public   void createHiveAndHbase() throws Exception {		
		List<StockMetaInfo> list =new ArrayList<StockMetaInfo>();
		StockMetaInfo s=new StockMetaInfo();
		s.setSt_id("2736");
		s.setCreate_script_hive("create table TB_BILL_PAPER_DISCOUNT(BUSINESS_ID string,DISCOUNT__DATE string,DISCOUNT_OUT_NAME string,DISCOUNT_IN_NAME string,FXBS_TIME_MARK string,ROW_NUM double) row format delimited fields terminated by ',' stored as textfile;");
		s.setTable_name("TB_BILL_PAPER_DISCOUNT");
		s.setIs_partition("0");
		s.setRow_num(0+"");
		
		list.add(s);
		
		HiveConnection hc = new HiveConnection();
		MataStockLog msl=null;
		for (StockMetaInfo smi : list) {
			
			msl=new MataStockLog();
			
			msl.setBegin_time(System.currentTimeMillis()+"");
			//获取hbase
			//String hbase = smi.getCreate_script_hbase();			
			String hive = smi.getCreate_script_hive();
			//创建hive
			hc.execute(hive);
			//创建hbase脚本
			//HbaseUtils.createTable(hbase);
			String partition = "";
			//创建分区脚本
			if (smi.getIs_partition().equals("1")) {// '1是分区表，0为不是分区',
				partition = smi.getAdd_partition_script();
				hc.execute(partition);
			}
			
			//导入数据//
			//SqoopImport.impTableData(smi);
			//校验数据
			
			int source_rownum=Integer.parseInt(smi.getRow_num());
			int target_rownum= new HiveUtils().rowCount(smi.getTable_name());
			
			msl.setSource_rownum(source_rownum+"");//源数据条数
			msl.setTarget_rownum(target_rownum+"");//目标数据条数
			int status=(source_rownum!=target_rownum)?0:1;
			msl.setStatus(status+"");
			
			msl.setEnd_time(System.currentTimeMillis()+"");
			
			
			//记录完成信息
			StockMetaInfoDAO.insertLogInfo(smi, msl);
			
			
		}
	}
}
