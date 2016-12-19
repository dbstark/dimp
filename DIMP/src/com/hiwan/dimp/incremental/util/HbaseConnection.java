package com.hiwan.dimp.incremental.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseConnection {

	static Configuration conf = null ;
	
	static HTable incredate_table  ;
	static int countIncredate ;
	static List<Put> incredatelist = new ArrayList<Put>();
	
	static{
		conf = HBaseConfiguration.create();
		try {
			if(incredate_table == null){
				incredate_table = new HTable(conf, "tb_table_increment_date");
			}
			incredate_table.setAutoFlush(false);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void putDownloaded(String table_name , String file_date ){
		countIncredate++;
		String major_key = table_name ;
		Put put = new Put(Bytes.toBytes(major_key));
		put.add(Bytes.toBytes("f"), Bytes.toBytes("date") , Bytes.toBytes(file_date));
		incredatelist.add(put);
		if(countIncredate%100 == 0){
			try {
				incredate_table.put(incredatelist);
				incredate_table.flushCommits();
				incredatelist.clear();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if(countIncredate%1000 == 0){
			System.out.println("table_name=" + table_name);
		}
	} 
	
	public void putDownloaded(String table_name , String file_date , String cim_job_name){
		countIncredate++;
		String major_key = cim_job_name ;
		Put put = new Put(Bytes.toBytes(major_key));
		put.add(Bytes.toBytes("f"), Bytes.toBytes("table_name") , Bytes.toBytes(table_name));
		put.add(Bytes.toBytes("f"), Bytes.toBytes("date") , Bytes.toBytes(file_date));
		incredatelist.add(put);
		if(countIncredate%100 == 0){
			try {
				incredate_table.put(incredatelist);
				incredate_table.flushCommits();
				incredatelist.clear();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if(countIncredate%1000 == 0){
			System.out.println("cim_job_name=" + cim_job_name);
		}
	}

	public void putAll(){
		try {
			incredate_table.put(incredatelist);
			incredatelist.clear();
			
			incredate_table.flushCommits();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		
		HbaseConnection hbase_conn = new HbaseConnection() ;
		
		
	}
	
}
