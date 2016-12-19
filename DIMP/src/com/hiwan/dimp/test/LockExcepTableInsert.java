package com.hiwan.dimp.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import com.hiwan.dimp.hive.HiveConnection;

public class LockExcepTableInsert {
	
	public static void main(String[] args) {
		if(args == null || args.length < 1){
			System.out.println("请输入表列表");
			System.exit(0) ;
		}
		LockExcepTableInsert leti = new LockExcepTableInsert() ;
		try {
			leti.insert_lock_table(args[0]) ;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void insert_lock_table(String lock_table_file) throws Exception{
		List<String> list = read_lock_table(lock_table_file) ;
		ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(5);
		LinkedBlockingQueue<String> table_quene = new LinkedBlockingQueue<String>() ;
		for(String s : list){
			table_quene.add(s) ;
		}
		LockExceThread let = null ;
		for(int i = 0 ; i < 10 ; i++){
			let = new LockExceThread(table_quene) ;
			pool.submit(let) ;
		}
		pool.shutdown() ;
	}
	
	public List<String> read_lock_table(String lock_table_path) throws Exception{
		
		File file = new File(lock_table_path) ;
		List<String> table_list = new ArrayList<String>() ;
		
		BufferedReader br = new BufferedReader(new FileReader(file)) ;
		String line = "" ;
		while( (line=br.readLine())!= null){
			table_list.add(line) ;
		}
		br.close() ;
		return table_list ;
	}
	
	class LockExceThread implements Serializable,Runnable{
		
		private static final long serialVersionUID = 1L;
		LinkedBlockingQueue<String> table_quene ;

		public LockExceThread(LinkedBlockingQueue<String> table_quene) {
			super();
			this.table_quene = table_quene;
		}

		@Override
		public void run() {
			// TODO Auto-generated method stub
			String table_name = "" ;
			HiveConnection hive_conn = new HiveConnection() ;
			while(true){
				if(table_quene.size() > 0){
					table_name = table_quene.poll() ;
					hive_conn.execute(" truncate table " + table_name ) ;
					hive_conn.execute(" insert into table " + table_name + " select * from " + table_name + "_hivetemp") ;
				}else{
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
