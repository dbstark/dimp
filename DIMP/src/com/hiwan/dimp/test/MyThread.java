package com.hiwan.dimp.test;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;

public class MyThread extends Thread {

	String thread_name ;
	int thread_value ;

	public MyThread() {
		super();
	}

	public MyThread(String thread_name, int thread_value) {
		super();
		this.thread_name = thread_name;
		this.thread_value = thread_value;
	}
	
	@Override
	public void run() {
		synchronized (ThreadPoolTest.thread_map) {
			System.out.println("线程结束前:"+ThreadPoolTest.thread_map);
			int number = new Random().nextInt(20) + 1;
			try {
				System.out.println("thread_name: "+thread_name+"   Random number:"+number);
				Thread.sleep(number*1000);				
				 Iterator<Entry<String, Integer>> it = ThreadPoolTest.thread_map.entrySet().iterator();
			     while(it.hasNext()){
			            Entry<String, Integer> entry = it.next();
			            String key = entry.getKey();
			            if(key == thread_name){			           	 
			           	 it.remove();    
			            }
			     }
				// ThreadPoolTest.thread_map.remove(thread_name) ;	
				
				System.out.println("结束线程后:"+ThreadPoolTest.thread_map);
			} catch (InterruptedException e) {
			 
			}
			
		}
		
		
		
	}

	public String getThread_name() {
		return thread_name;
	}

	public void setThread_name(String thread_name) {
		this.thread_name = thread_name;
	}

	public int getThread_value() {
		return thread_value;
	}

	public void setThread_value(int thread_value) {
		this.thread_value = thread_value;
	}


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String name = "1.dat.gz.D" ;
		
		System.out.println(name.endsWith(".dat.gz.D"));
	}

}
