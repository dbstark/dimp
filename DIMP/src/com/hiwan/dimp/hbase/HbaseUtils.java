package com.hiwan.dimp.hbase;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.hiwan.dimp.db.DBAccess;

public class HbaseUtils {

	private static Configuration conf = null ;
	
	private static HTable test ;
	private static int testNum  = 0 ;
	private static List<Put> testPuts = new ArrayList<Put>();
	
	private static HTable test2 ;
	private static int test2Num  = 0 ;
	private static List<Put> test2Puts = new ArrayList<Put>();

	private static String clientPort ;
	private static String quorum;
	private static String parent;
	//连接
	static {
		
		conf= HBaseConfiguration.create() ;
		Properties prop = new Properties();
		try {
			prop.load(DBAccess.class.getClassLoader().getResourceAsStream("hbase_config.properties"));
			clientPort= prop.getProperty("hbase.zookeeper.property.clientPort");
			quorum= prop.getProperty("hbase.zookeeper.quorum");
			parent= prop.getProperty("zookeeper.znode.parent");
			conf.set("hbase.zookeeper.property.clientPort", clientPort); 
			conf.set("hbase.zookeeper.quorum", quorum); 
			conf.set("zookeeper.znode.parent", parent);
		} catch (Exception e) {
			e.printStackTrace();
		}		 			
	}
	
	static byte[] f = "f".getBytes(); // f列族
	static byte[] f2 = "f2".getBytes(); // f2列族
	static byte[] df = "df".getBytes(); // df列族
	
	static byte[] name = "name".getBytes() ;
	static byte[] name1 = "name1".getBytes() ;
	static byte[] name2 = "name2".getBytes() ;
	static byte[] name3 = "name3".getBytes() ;
	static byte[] sex = "sex".getBytes() ;
	
	//删除hbase表
	public static void dropTable(String tableName){
		HBaseAdmin hba = null ;
		try {
			hba = new HBaseAdmin(conf);
			if(hba.tableExists(tableName)){
				hba.disableTable(tableName) ;
				hba.deleteTable(tableName) ;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	//创建表
	public static void createTable(String tableName){
				createTable(tableName , new String[]{"cf"});
				
	}
	//创建表
	public static void createTable(String tableName , String[] familys){
		HBaseAdmin hba = null ;
		HTableDescriptor htd = null ;
		try {
			hba = new HBaseAdmin(conf);
			if(hba.tableExists(tableName)){
				hba.disableTable(tableName) ;
				hba.deleteTable(tableName) ;
			}
			htd = new HTableDescriptor(tableName) ;
			if (familys != null && familys.length > 0) {
				for (String family : familys) {
					htd.addFamily(new HColumnDescriptor(family));
				}
			}
			hba.createTable(htd) ;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	/* 对于test的数据,插入到test*/
	public static void put_test(String rowkey , String family , String column , String value ) throws UnsupportedEncodingException{
		testNum ++ ;
		Put put = new Put(rowkey.getBytes()) ;
		put.add(family.getBytes(), column.getBytes(), value.getBytes("UTF-8")) ;
		testPuts.add(put) ;
		if(testNum%100 == 0){
			try {
				test.put(testPuts);
				test.flushCommits();
				testPuts.clear();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if(testNum%5000==0){
			System.out.println("rowkey=" + rowkey);
		}
	}
	
	/* test插入全部数据*/
	public static void put_all_test(){
		try {
			test.put(testPuts);
			testPuts.clear();
			test.flushCommits();			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/* 插入到test2*/
	public static void put_test2(String rowkey , String family , String column , String value ){
		test2Num ++ ;
		Put put = new Put(rowkey.getBytes()) ;
		put.add(family.getBytes(), column.getBytes(), value.getBytes()) ;
		test2Puts.add(put) ;
		if(test2Num%100 == 0){
			try {
				test2.put(test2Puts);
				test2.flushCommits();
				test2Puts.clear();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if(test2Num%5000==0){
			System.out.println("rowkey=" + rowkey);
		}
	}
	
	/* test2插入全部数据*/
	public static void put_all_test2(){
		try {
			test2.put(test2Puts);
			test2Puts.clear();
			test2.flushCommits();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/* 删除单元格数据*/
	public static void deleteByRowkey( String tableName , String rowkey ){
		HTable ht = null ;
		Delete d = null ;
		try {
			ht = new HTable(conf, tableName) ;
			d = new Delete(rowkey.getBytes()) ;
			ht.delete(d) ;
			ht.close() ;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static Result getByVersion(int version , String tableName , String rowkey){
		Get get = null ;
		Result result = null ;
		HTable table = null ;
		try {
			get = new Get(rowkey.getBytes()) ;
			get.setMaxVersions(version) ; //设置可以获取的最高的版本数
			table = new HTable(conf, tableName) ;
			result = table.get(get);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result ;
	}
	
	//获取hbase表的名称中的全部数据  scan
	public static ResultScanner getTableResult(String tablename){
		Scan scan = new Scan() ;
		ResultScanner rs = null ;
		HTable table = null ;
		try {
			table = new HTable(conf, tablename.getBytes());
			rs = table.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return rs;
	}
	
	//根据主键找到Result
	public static Result getResult(String rowkey , String tablename){
		Get get = new Get(rowkey.getBytes()) ;
		Result result = null ;
		HTable table = null ;
		try {
			table = new HTable(conf, tablename) ;
			result = table.get(get);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result ;
	}

	public static void truncateTable(String tableName, String[] familys)
			throws Exception {
		HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
		// 如果存在要创建的表，那么先删除，再创建
		if (hBaseAdmin.tableExists(tableName)) {
			hBaseAdmin.disableTable(tableName);
			hBaseAdmin.deleteTable(tableName);
		}
		HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
		// 设置表中的列族
		if (familys != null && familys.length > 0) {
			for (String family : familys) {
				tableDescriptor.addFamily(new HColumnDescriptor(family));
			}
		}
		// 创建表
		hBaseAdmin.createTable(tableDescriptor);
	}
	
	//查询，指定条件
	public static void selectByFilter(String tableName , List<String> filters){
		HTable ht = null ;
		FilterList fl = null ;
		Scan scan = new Scan() ;
		ResultScanner rs = null ;
		try {
			ht = new HTable(conf, tableName) ;
			fl = new FilterList() ;
			for(String s : filters){
				String[] arr = s.split(",") ;
				//family , qualifier , value
				fl.addFilter(new SingleColumnValueFilter(Bytes.toBytes(arr[0]), Bytes.toBytes(arr[1]), CompareOp.EQUAL, Bytes.toBytes(arr[2]))) ;
				
			}
			scan.setFilter(fl) ;
			rs = ht.getScanner(scan) ;
			for(Result r : rs){
				for(KeyValue kv : r.list()){
					System.out.println("row : "+new String(kv.getRow()));
					System.out.println("family : "+new String(kv.getFamily()));
	                System.out.println("column : "+new String(kv.getQualifier()));  
	                System.out.println("value : "+new String(kv.getValue()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static void main(String[] args) {
		ResultScanner rs = HbaseUtils.getTableResult("T00_SAVPRO_INTRST") ;
		for(Result result : rs){
			System.out.println(new String(result.getRow()));
		}
	}

}
