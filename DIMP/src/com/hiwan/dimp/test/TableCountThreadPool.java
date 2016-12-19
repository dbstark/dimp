package com.hiwan.dimp.test;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import com.hiwan.dimp.hive.HiveConnection;

public class TableCountThreadPool {
	
	
	public static void main(String[] args) {
		TableCountThreadPool tctp = new TableCountThreadPool() ;
		try {
			tctp.table_count();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public void table_count() throws Exception{
		
		HiveConnection hive_conn = new HiveConnection() ;
		ResultSet rs = hive_conn.execute("show tables") ;
		String table_name = "" ;
		LinkedBlockingQueue<String> table_quene = new LinkedBlockingQueue<String>() ;
		while(rs.next()){
			table_name = rs.getString(1) ;
			table_quene.add(table_name) ;
		}
		hive_conn.close(); 
		TableCountThread tch = null ;
		ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(5);
		for(int i = 0 ; i < 5 ; i++){
			tch = new TableCountThread(table_quene) ;
			pool.submit(tch) ;
		}
		pool.shutdown(); 
	}
	
	
	class TableCountThread implements Runnable,Serializable {

		private LinkedBlockingQueue<String> table_quene ;
		
		public TableCountThread(LinkedBlockingQueue<String> table_quene) {
			super();
			this.table_quene = table_quene;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			HiveConnection hive_conn = new HiveConnection() ;
			String table_name = "" ;
			ResultSet rs = null ;
			String count_num = "" ;
			while(true){
				if(table_quene.size() > 0){
					try {
						table_name = table_quene.poll() ;
						rs = hive_conn.execute("select count(*) from " + table_name) ;
						while(rs.next()){
							count_num = rs.getString(1) ;
						}
						System.out.println("right:table_name:" + table_name + "\t" + "count_num:" + count_num);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						System.out.println("error:table_name:" + table_name + "\t" ) ;
//						e.printStackTrace();
					}
				}else{
					break ;
				}
			}
			hive_conn.close(); 
		}
	}
	
}
