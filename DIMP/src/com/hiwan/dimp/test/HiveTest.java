package com.hiwan.dimp.test;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;

import com.hiwan.dimp.bean.StockMetaInfo;
import com.hiwan.dimp.dao.PageTool;
import com.hiwan.dimp.hive.HiveConnection;

public class HiveTest {
	/**
	 * 
	 * @Describe:hbase建表测试
	 *  @time  :2015年7月9日 上午10:18:51
	 */
	//@Test 
	public void hiveCreateTable() throws SQLException{
		StockMetaInfo smi=PageTool.getMetafo("ACCINF");
		System.out.println(smi);
		//PageTool.insertMetaInfo(smi);
		HiveConnection hc=new HiveConnection();
		hc.execute(smi.getCreate_script_hive());
	}
	/**
	 * 测试hive表记录数
	 *  @time  :2015年7月10日 上午11:10:33
	 */
	@Test 
	public void hiveRowCount() throws SQLException{
		//HiveUtils ho=new HiveUtils();
		//int a=ho.rowCount("tmp_xxj3");
		//System.out.println(a);
		
		HiveConnection hc=new HiveConnection();
		ResultSet rs=hc.execute("select count(1)  from TB_BILL_PAPER_DISCOUNT");
		int rowCount=0;
		while(rs.next()){
			rowCount=rs.getInt(1);
			//System.out.println(rs.getString("records"));
		}
		System.out.println(rowCount);
	}	
	
}
