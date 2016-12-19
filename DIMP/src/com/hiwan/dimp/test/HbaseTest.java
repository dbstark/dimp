package com.hiwan.dimp.test;



import com.hiwan.dimp.hbase.HbaseUtils;

public class HbaseTest {
	/**
	 * 
	 *  @Describe:hbase建表测试
	 *  @time  :2015年7月9日 上午10:18:51
	 */
	public void hbaseCreateTable(){
		HbaseUtils.createTable("ACCINF");
		//HbaseUtils.getTableResult("T00_SAVPRO_INTRST") ;
	}
	
	
	public static void main(String[] args) {
		HbaseUtils.createTable("BB");
	}
	
}
