package com.hiwan.dimp.test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;


public class ThreadPoolTest {

	//public static Map<String, Integer> thread_map = new HashMap<String, Integer>() ;
	public static Map<String, Integer> thread_map=new ConcurrentHashMap<String, Integer>() ;
	/**
	 * @param args
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException {
		long start=System.currentTimeMillis();
		 
		// TODO Auto-generated method stub
		//最大map数量
		int map_num = 15 ;
		//需要启动的所有sqoop shell的列表  sqoop - map
		Map<String, Integer> map = new HashMap<String, Integer>() ;
		map.put("a", 11) ;
		map.put("b", 4) ;
		map.put("c", 3) ;
		map.put("d", 7) ;
		map.put("e", 5) ;
		map.put("f", 9) ;
		map.put("g", 6) ;
		map.put("h", 8) ;
		map.put("i", 10) ;
		map.put("j", 2) ;
		map.put("k", 2) ;
		//建立最大数量为最大map数量的线程池
		ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(map_num) ;
		
		//对sqoop shell列表进行循环
		int thread_num = 0 ;
		String thread_name = "" ;
		
		int pool_now_num = 0 ;
		MyThread thread = null ;
		boolean flag = true ;
		for(Entry<String, Integer> entry : map.entrySet()){
			 
			thread_num = entry.getValue() ;
			thread_name = entry.getKey() ;
			//pool_now_num += thread_num ;
			 //Map<String, Integer> tt = Collections.synchronizedMap(thread_map);
			// System.out.println(tt);
			Iterator<Map.Entry<String, Integer>> it = thread_map.entrySet().iterator();
			  while (it.hasNext()) {
			    Map.Entry<String, Integer> entry1 = it.next();
			    pool_now_num += entry1.getValue() ;
			  }
			
			
			//判断
				//对thread_map情况进行循环判断
				//新建一个Map对象用于验证现在使用的map数量
			 
				while (map_num < pool_now_num +thread_num) {	
					//System.out.println("----- pool_now_num"+pool_now_num+ " thread_num:"+thread_num +" pool_now_num +thread_num"+(pool_now_num +thread_num));				 
					if(pool.getPoolSize() > 0){
						
						int count_num = 0 ;
						 // Map<String, Integer> hh = new  HashMap<String, Integer>(thread_map) ;
						 
						 //Map<String, Integer> hh = Collections.synchronizedMap(thread_map);  
						// System.out.println(hh);
						 Iterator<Map.Entry<String, Integer>> it1 = thread_map.entrySet().iterator();
						  while (it1.hasNext()) {
						    Map.Entry<String, Integer> entry1 = it1.next();
						    count_num += entry1.getValue() ;
						  }
						//System.out.println("****** count_num:"+count_num +"   thread_num:"+thread_num);
						if(map_num > count_num + thread_num){
							pool_now_num = count_num ;
						//	System.out.println("$$$$$$$$$ count_num2:"+pool_now_num);
							break ;
						}
						//Thread.sleep(1*1000) ;
						//System.out.println("sleep");
					}else{
						break ;
					}
				}
			System.out.println("pool size:"+pool.getPoolSize());
			System.out.println("pool active size:"+pool.getActiveCount());
			thread_map.put(thread_name, thread_num) ;
			thread = new MyThread(thread_name, thread_num) ;
			pool.submit(thread) ;
			//Thread.sleep(1*1000) ;
			//System.out.println(pool.getPoolSize());
		}
		//关闭线程池
		pool.shutdown() ;
		
		long end=System.currentTimeMillis();
		System.out.println("end :"+(end-start)/1000);
	}	
	
}
