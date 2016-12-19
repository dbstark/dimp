package com.hiwan.dimp.tool;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class TableLineCount {

	
	public void table_count(String file_path) throws Exception{
		
		BufferedReader br = new BufferedReader(new FileReader(new File(file_path))) ;
		String line = "" ;
		LinkedBlockingQueue<String> table_quene = new LinkedBlockingQueue<String>() ;
		
		while( (line = br.readLine()) != null ){
			table_quene.add(line) ;
		}
		br.close() ;
		TableLineCountThread table_count_thread = null ;
		ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(10) ;
		
		for(int i = 0 ; i < 10 ; i++){
			table_count_thread = new TableLineCountThread(table_quene) ;
			pool.submit(table_count_thread) ;
		}
		pool.shutdown() ;
	}
	
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args == null || args.length ==0){
			System.out.println("请输入表名文件");
			System.exit(1) ;
		}
		TableLineCount table_count = new TableLineCount() ;
		table_count.table_count(args[0]) ;
	}
	
	
	class TableLineCountThread implements Runnable , Serializable{

		private LinkedBlockingQueue<String> table_quene ;

		public TableLineCountThread(LinkedBlockingQueue<String> table_quene) {
			super();
			this.table_quene = table_quene;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			String table_name ;
			String sql ;
			ResultSet rs ;
			HiveConnection hive_conn = new HiveConnection() ;
			while(true){
				table_name = table_quene.poll() ;
				sql = " select " + table_name +  ", count(*)  from " + table_name  ;
				rs = hive_conn.execute(sql) ;
				try {
					if(rs.next()){
						System.out.println(rs.getString(1) + "\t" + rs.getString(2));
					}
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if(table_quene.size() == 0){
					break ;
				}
			}
		}

		public LinkedBlockingQueue<String> getTable_quene() {
			return table_quene;
		}

		public void setTable_quene(LinkedBlockingQueue<String> table_quene) {
			this.table_quene = table_quene;
		}
		
		
		
		
	}

}
