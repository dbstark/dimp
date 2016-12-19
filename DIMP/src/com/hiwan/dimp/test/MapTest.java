package com.hiwan.dimp.test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class MapTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		//不会报错
		Map<String, Integer> map = new HashMap<String, Integer>() ;
		map.put("a", 11) ;
		map.put("b", 4) ;
		map.put("c", 3) ;
		
		Map<String, Integer> map2 = new HashMap<String, Integer>(map) ;
		map.remove("a") ;
		
		for(Map.Entry<String, Integer> entry : map.entrySet()){
			System.out.println(entry.getKey());
		}
		System.out.println("=================================================");
		/*for(Map.Entry<String, Integer> entry : map2.entrySet()){
			System.out.println(entry.getKey());
		}*/
		Iterator<Entry<String, Integer>> it = map2.entrySet().iterator() ;
		while(it.hasNext()){
			Map.Entry entry = it.next() ;
			System.out.println(entry.getKey());
		}
		
	}

}
