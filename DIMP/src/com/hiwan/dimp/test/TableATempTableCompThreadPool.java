package com.hiwan.dimp.test;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import com.hiwan.dimp.hive.HiveConnection;

public class TableATempTableCompThreadPool {
	
	
	public static void main(String[] args) {
		/*TableATempTableCompThreadPool ttt = new TableATempTableCompThreadPool() ;
		try {
			ttt.table_count() ;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		
		String sql = "select count(*) from (select * from " + "table" + " except all select * from " + "table_temp" + ") t " ;
		System.out.println(sql);
		System.out.println("test");
	}

	public void table_count() throws Exception{
		
		HiveConnection hive_conn = new HiveConnection() ;
		ResultSet rs = hive_conn.execute("show tables") ;
		String table_name = "" ;
		LinkedBlockingQueue<TableATableTemp> table_a_tmp_quene = new LinkedBlockingQueue<TableATableTemp>() ;
		List<String> table_list = new ArrayList<String>() ;
		while(rs.next()){
			table_name = rs.getString(1) ;
			table_list.add(table_name) ;
		}
		String table_tmp_name = "" ;
		for(String table : table_list){
			table_tmp_name = table + "_hivetemp" ;
			if(table_list.contains(table_tmp_name)){
				table_a_tmp_quene.add(new TableATableTemp(table, table_tmp_name)) ;
			}
		}
		hive_conn.close(); 
		TableATempCompareThread tatt = null ;
		ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(5);
		for(int i = 0 ; i < 5 ; i++){
			tatt = new TableATempCompareThread(table_a_tmp_quene) ;
			pool.submit(tatt) ;
		}
		pool.shutdown(); 
	}
	
	class TableATempCompareThread implements Runnable,Serializable {

		private LinkedBlockingQueue<TableATableTemp> table_quene ;
		
		public TableATempCompareThread(
				LinkedBlockingQueue<TableATableTemp> table_quene) {
			super();
			this.table_quene = table_quene;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			HiveConnection hive_conn = new HiveConnection() ;
			TableATableTemp table_a_temp = null ;
			ResultSet rs = null ;
			String count_num = "" ;
			while(true){
				if(table_quene.size() > 0){
					try {
						table_a_temp = table_quene.poll() ;
						//select count(*) from (select *　from table_name except all select * from table_name_temp)t;
						rs = hive_conn.execute("select count(*) from (select *　from " + table_a_temp.getTable_name()
								+ " except all select * from " + table_a_temp.getTable_tmp_name() + ") t " ) ;
						while(rs.next()){
							count_num = rs.getString(1) ;
						}
						System.out.println("right:" + table_a_temp.getTable_name() + "\t" + table_a_temp.getTable_tmp_name() + "\t" + "count_num:" + count_num);
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						System.out.println("error:" + table_a_temp.getTable_name() + "\t" + table_a_temp.getTable_tmp_name() + "\t" + "count_num:" + count_num) ;
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
