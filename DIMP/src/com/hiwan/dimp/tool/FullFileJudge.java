package com.hiwan.dimp.tool;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 用于判断文件是否解密完全
 * */
public class FullFileJudge {
	
	public List<String> file_judge() throws Exception{
		String dir_path = "/home/jshdata_increment/data/code" ;
		String out_path = "/home/jshdata_increment/log/" ;
		List<String> list = new ArrayList<String>() ;
		FullFileJudgeDao ffjdao = new FullFileJudgeDao() ;
		List<String> file_list = new ArrayList<String>() ;
		Map<String, List<String>> map = new HashMap<String, List<String>>() ;
		
		BufferedWriter bw = null ;
		
		//获取job_table对应关系
		Map<String, String> job_table_map = ffjdao.job_table_map_from_hive() ;
		//获取增量导入中所有table的名称
		List<String> table_name_list = ffjdao.table_list() ;
		//获取job--table--date对应关系
		Map<String, String> job_date = ffjdao.cim_job_date() ;
		
		//获取code目录下文件列表
		File dir_file = new File(dir_path) ;
		File[] arr_file = dir_file.listFiles() ;
		
		for(File file : arr_file){
			String file_path = file.getPath() ;
			String file_name = file.getName() ;
			String job_name = "";
			try {
				job_name = file_name.substring(0 , file_name.indexOf("3200")-1).toUpperCase();
			} catch (Exception e) {
				System.out.println(file_name);
				continue ;
			}
			String[] f_arr = file_name.split("_") ;
			String file_date = f_arr[f_arr.length -3] ;
			
			if(map.containsKey(file_date)){
				file_list = map.get(file_date) ;
			}else{
				file_list = new ArrayList<String>() ;
			}
			if(job_date.containsKey(job_name)){
				if(file_date.compareTo(job_date.get(job_name)) > 0){
					file_list.add(file_path) ;
				}
			}else{
				String table_name = job_table_map.get(job_name) ;
				if(job_table_map.containsKey(job_name) && table_name_list.contains(table_name)){
					file_list.add(file_path) ;
				}
			}
			map.put(file_date, file_list) ;
		}
		for(Entry<String, List<String>> entry : map.entrySet()){
			bw = new BufferedWriter(new FileWriter(new File(out_path + entry.getKey()))) ;
			for(String s : entry.getValue()){
				bw.write(s + "\n") ;
			}
			bw.close() ;
		}
		return list ;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		FullFileJudge ffj = new FullFileJudge() ;
		try {
			ffj.file_judge() ;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
