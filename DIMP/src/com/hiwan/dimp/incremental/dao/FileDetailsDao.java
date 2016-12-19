package com.hiwan.dimp.incremental.dao;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import com.hiwan.dimp.incremental.bean.FileDetailsBean;

public class FileDetailsDao {
	
	public static void create_directory(String path){
		File file = new File(path) ;
		if(!file.exists() && !file.isDirectory()){
			System.out.println(path + ":目录不存在 \n创建");
			file.mkdir() ;
		}else{
			System.out.println(path + ":目录存在");
		}
	}

	public static FileDetailsBean obtain_file_details(int column_num , String source_path , String abnormal_path) throws Exception{
		FileDetailsBean fdb = null ;
		
		File f = new File(source_path) ;
		String abnormal_file = abnormal_path + "/" + f.getName() ;
		System.out.println("文件转码:"+source_path);
		String[] arr = source_path.split("\\.") ;
		String out_path = arr[0] + "_MID." + arr[1] ;
		BufferedReader br = null ;
		BufferedWriter bw = null ;
		BufferedWriter bw_abnormal = null ;
		String line = "" ;
		int line_num = 0 ;
		int abnormal_line_num = 0 ;
		String more_line = "" ;
		String right_wrong = "" ;
		br = new BufferedReader(new InputStreamReader(new FileInputStream(source_path), "GBK")) ;
		bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out_path), "UTF-8")) ;
		
		while ((line = br.readLine()) != null) {
			line = line.replace("\r", "").trim().replaceAll("\\s*!\\s*", "!") ;
			if(line.endsWith("!")){
				line_num += 1 ;
				more_line = more_line + line ;
				int dat_num = (more_line+"a").split("!").length-1 ;
				if( column_num !=  dat_num){
					if(bw_abnormal == null){
						create_directory(abnormal_path) ;
						bw_abnormal =  new BufferedWriter(new OutputStreamWriter(new FileOutputStream(abnormal_file), "UTF-8")) ;
					}
					bw_abnormal.write(line_num + "\t" + column_num + "\t" + dat_num + "\t" + more_line + "\n") ;
					abnormal_line_num += 1 ;
				}/*else{
					bw.write(more_line + "\n") ;
				}*/
				bw.write(more_line + "\n") ;
				more_line = "" ;
			}else{
				more_line = more_line + line ;
			}
		}
		if("".equals(more_line)){
			right_wrong = "right" ;
		}else{
			right_wrong = "error" ;
		}
		try {
			if(bw != null){
				bw.close() ;
			}
			if(br != null){
				br.close() ;
			}
			if(bw_abnormal != null){
				bw_abnormal.close() ;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		fdb = new FileDetailsBean(abnormal_line_num, line_num, (double)abnormal_line_num/(double)line_num, right_wrong) ;
		return fdb ;
	}
	
	/**
	 * 转码：把GB2312文件转换为UTF-8文件
	 * 并把转换后的文件名称输出出来
	 * */
	public static String file_code_change(String file_path){
		System.out.println("文件转码:"+file_path);
		String[] arr = file_path.split("\\.") ;
		String out_path = arr[0] + "_MID." + arr[1] ;
		BufferedReader br  ;
		BufferedWriter bw ;
		String line = "" ;
		String more_line = "" ;
		String right_error = "" ;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(file_path), "GBK")) ;
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out_path), "UTF-8")) ;
			
			while ((line = br.readLine()) != null) {
				line = line.replace("\r", "").trim().replaceAll("\\s*!\\s*", "!") ;
				if(line.endsWith("!")){
					more_line = more_line + line ;
					bw.write(line + "\n") ;
					more_line = "" ;
				}else{
					more_line = more_line + line ;
				}
			}
			if("".equals(more_line)){
				right_error = "right" ;
			}else{
				right_error = "error" ;
			}
			bw.close() ;
			br.close() ;
		} catch (Exception e) {
			e.printStackTrace();
		} 
		return right_error ;
	}
	
	public static void main(String[] args){
		
		String line = "320501024 !603050024200806909 !1 !20160326 !03 !156 !992.38 !13733.14 !31087.16 !31087.16 !32 !20160401 !" ;
		System.out.println(line.replace("\r", "").trim().replaceAll("\\s*!\\s*", "!"));
		
	}
	
	
}
