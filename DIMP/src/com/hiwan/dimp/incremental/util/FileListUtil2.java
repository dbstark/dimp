package com.hiwan.dimp.incremental.util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.channels.FileChannel;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.hiwan.dimp.incremental.bean.AugmentInfo;
import com.hiwan.dimp.incremental.bean.IncrementDate;
import com.hiwan.dimp.incremental.bean.SourceFileBean;

/**
 * 获取数据文件夹,list下面的所有文件,并且把完成的数据文件mv到数据仓库中
 * */
public class FileListUtil2 {

	/**
	 * list file
	 * */
	public List<String> list_data_file(String path){
		List<String> list_file = new ArrayList<String>() ;
		
		list_file = getFile(path, list_file) ;
		
		return list_file ;
	}
	
	public Map<String, List<SourceFileBean>> map_data_file(String path,
			Map<String, String> job_table_map, String save_path,
			Map<String, AugmentInfo> aug_info_map,
			Map<String, IncrementDate> incre_date_map) {
		Map<String,List<SourceFileBean>> map_file = new HashMap<String, List<SourceFileBean>>() ;
		Map<String, String> time_map = new HashMap<String, String>() ;
		/**job -- table
		 * */
		map_file = getFile(path, map_file , job_table_map , save_path,aug_info_map,incre_date_map,time_map) ;
		
		//对map_file进行转码
		for(Entry<String, List<SourceFileBean>> entry : map_file.entrySet()){
			for(SourceFileBean sfb : entry.getValue()){
				file_code_change(sfb.getSource_path()) ;
			}
		}
		return map_file ;
	}
	
	public List<String> getFile(String path , List<String> list){
		File file = new File(path) ;
		File[] arr_file = file.listFiles() ;
		for(File f : arr_file){
			if(f.isFile()){
				//对文件名和后缀名的格式进行判断
				String file_path = f.getPath() ;
				String file_name = f.getName() ;
				if(file_name.indexOf("3200") > -1 && file_name.split("\\.").length > 1 && "dat".equals(file_name.split("\\.")[1])){
					file_code_change(file_path) ;
					list.add(file_path) ;
				}
			}else{
				getFile(f.getPath(), list) ;
			}
		}
		return list ;
	}

	public Map<String, List<SourceFileBean>> getFile(String path,
			Map<String, List<SourceFileBean>> map,
			Map<String, String> job_table_map, String save_path,
			Map<String, AugmentInfo> aug_info_map,
			Map<String, IncrementDate> incre_date_map ,
			Map<String, String> time_map
			) {
		
		File file = new File(path) ;
		File[] arr_file = file.listFiles() ;
		if(arr_file == null || arr_file.length == 0){
			System.out.println("没有文件需要加载");
		}else{
			for(File f : arr_file){
				if(f.isFile()){
					//对文件名和后缀名的格式进行判断
					String file_path = f.getPath() ;
					String file_name = f.getName() ;
					if (System.currentTimeMillis() - f.lastModified() > 60 * 1000) {
						if( !(file_name.endsWith("_MID.dat"))){
							List<SourceFileBean> list = null ;
							if(file_name.indexOf("_") > -1 &&file_name.indexOf("3200") > -1 && file_name.split("\\.").length > 1 && "dat".equals(file_name.split("\\.")[1])){
								String job_name = file_name.substring(0 , file_name.indexOf("3200")-1) ;
								String table_name = "" ;
								if(job_table_map.containsKey(job_name)){
									table_name = job_table_map.get(job_name) ;
								}else{
									table_name = file_name.substring(file_name.indexOf("_")+1, file_name.indexOf("3200")-1) ;
								}
								AugmentInfo aug_info = aug_info_map.get(table_name);
								IncrementDate incre_date = incre_date_map.get(table_name) ;
								
								String[] f_arr = file_name.split("_") ;
								String file_date = f_arr[f_arr.length -3] ;
								
								if(aug_info instanceof AugmentInfo && "配置表".equals(aug_info.getTable_type())){
									if(map.containsKey(table_name)){
										String old_time = time_map.get(table_name) ;
										list = map.get(table_name) ;
										if(file_date.equals(old_time)){
											list.add(new SourceFileBean(file_path, file_code_change(file_path))) ;
										}else if(file_date.compareTo(old_time) > 0){
											for(SourceFileBean sfb : list){
												file_delete(sfb.getMid_source_path()) ;
												file_mv(sfb.getSource_path(), save_path);
											}
											list.clear() ;
											list.add(new SourceFileBean(file_path, file_name_change(file_path))) ;
											//先不进行转码  最后同意进行转码的操作
//											list.add(new SourceFileBean(file_path, file_code_change(file_path))) ;
											time_map.put(table_name, file_date) ;
										}else{
//											file_mv(file_path, save_path);
											continue ;
										}
										map.put(table_name, list) ;
									}else{
										list = new ArrayList<SourceFileBean>() ;
										list.add(new SourceFileBean(file_path, file_name_change(file_path))) ;
										//先不进行转码  最后同意进行转码的操作
//										list.add(new SourceFileBean(file_path, file_code_change(file_path))) ;
										map.put(table_name, list) ;
										time_map.put(table_name, file_date) ;
									}
								}else{
									if(f.length() > 0 ){
										if(aug_info instanceof AugmentInfo && "信息表".equals(aug_info.getTable_type()) ){
											//判断file_path的日期是否为incre_date的日期+1
											
											if(incre_date instanceof IncrementDate){
												if(file_date.equals(evaluate(incre_date.getFile_date()))){
													if(map.containsKey(table_name)){
														list = map.get(table_name) ;
														list.add(new SourceFileBean(file_path, file_name_change(file_path))) ;
														//先不进行转码  最后同意进行转码的操作
//														list.add(new SourceFileBean(file_path, file_code_change(file_path))) ;
														map.put(table_name, list) ;
													}else{
														list = new ArrayList<SourceFileBean>() ;
														list.add(new SourceFileBean(file_path, file_name_change(file_path))) ;
														//先不进行转码  最后同意进行转码的操作
//														list.add(new SourceFileBean(file_path, file_code_change(file_path))) ;
														map.put(table_name, list) ;
													}
												}
											}else{
												//判断文件日期的最早值
												if(map.containsKey(table_name)){
													list = map.get(table_name) ;
													String old_time = time_map.get(table_name) ;
													if(file_date.equals(old_time)){
														list.add(new SourceFileBean(file_path, file_name_change(file_path))) ;
														//先不进行转码  最后同意进行转码的操作
//														list.add(new SourceFileBean(file_path, file_code_change(file_path))) ;
													}else if(file_date.compareTo(old_time) > 0){
														continue ;
													}else{
														/*for(SourceFileBean sfb : list){
															file_delete(sfb.getMid_source_path()) ;
														}*/
														list.clear() ;
														list.add(new SourceFileBean(file_path, file_name_change(file_path))) ;
														//先不进行转码  最后同意进行转码的操作
//														list.add(new SourceFileBean(file_path, file_code_change(file_path))) ;
														time_map.put(table_name, file_date) ;
													}
													map.put(table_name, list) ;
												}else{
													list = new ArrayList<SourceFileBean>() ;
													list.add(new SourceFileBean(file_path, file_name_change(file_path))) ;
													//先不进行转码  最后同意进行转码的操作
//													list.add(new SourceFileBean(file_path, file_code_change(file_path))) ;
													map.put(table_name, list) ;
													time_map.put(table_name, file_date) ;
												}
											}
										} else { 
											if(map.containsKey(table_name)){
												list = map.get(table_name) ;
												list.add(new SourceFileBean(file_path, file_name_change(file_path))) ;
												//先不进行转码  最后同意进行转码的操作
//												list.add(new SourceFileBean(file_path, file_code_change(file_path))) ;
												map.put(table_name, list) ;
											}else{
												list = new ArrayList<SourceFileBean>() ;
												list.add(new SourceFileBean(file_path, file_name_change(file_path))) ;
												//先不进行转码  最后同意进行转码的操作
//												list.add(new SourceFileBean(file_path, file_code_change(file_path))) ;
												map.put(table_name, list) ;
											}
										}
									}else{
										file_mv(file_path, save_path);
									}
								}
//								map.put(file_path, file_code_change(file_path)) ;
							}
						}else{
							//把大小为0的文件移到save path
							if(!(file_name.endsWith("_MID.dat"))){
								file_mv(file_path, save_path);
							}else{
								file_delete(file_path) ;
							}
						}
					}
				}else{
					getFile(f.getPath(), map,job_table_map , save_path , aug_info_map,incre_date_map,time_map) ;
				}
			}
		}
		return map ;
	}
	
	public String evaluate(String date)  {
		String result = "";
		try {
			date = date.trim() ;
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd") ;
			Calendar c = Calendar.getInstance() ;
			c.setTime(sdf.parse(date)) ;
			int day=c.get(Calendar.DATE); 
			c.set(Calendar.DATE,day+1);
			result = sdf.format(c.getTime())  ;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result ;
	}
	
	
	/**
	 * mv file
	 * */
	public void file_mv(String file_path , String target_path){
		
		try {
			file_channel_copy(file_path, target_path) ;
			file_delete(file_path) ;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		file_delete(file_path) ;
		
	}
	
	public void file_mv(List<SourceFileBean> source_bean_list , String target_path){
		
		try {
			for(SourceFileBean source_bean : source_bean_list){
				file_channel_copy(source_bean.getMid_source_path(), target_path) ;
				file_delete(source_bean.getMid_source_path()) ;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		file_delete(file_path) ;
	}
	
	
	
	public void file_mv2(List<SourceFileBean> source_bean_list , String target_path){
		
		try {
			for(SourceFileBean source_bean : source_bean_list){
				file_channel_copy(source_bean.getSource_path(), target_path) ;
				file_delete(source_bean.getSource_path()) ;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		file_delete(file_path) ;
	}
	
	public void file_mv3(List<SourceFileBean> source_bean_list , String target_path){
		
		try {
			for(SourceFileBean source_bean : source_bean_list){
				file_channel_copy(source_bean.getSource_path(), target_path) ;
				file_delete(source_bean.getMid_source_path()) ;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		file_delete(file_path) ;
	}
	
	
	public String list_to_string(List<SourceFileBean> source_bean_list , String target_path_day){
		String all_path = "" ;
		for(SourceFileBean source_bean : source_bean_list){
			all_path += target_path_day + "/" + new File(source_bean.getMid_source_path()).getName() + ";" ;
		}
		return all_path ;
		
	}

	/**
	 * copy file
	 * @throws Exception 
	 * */
	public void file_channel_copy(String file_path , String target_path) throws Exception{
		File now_file = new File(file_path) ;
		File target_file = new File(target_path + "/" + now_file.getName()) ;
		
		FileInputStream fis = new FileInputStream(now_file) ;
		FileOutputStream fos = new FileOutputStream(target_file) ;
		
		FileChannel in = fis.getChannel() ;
		FileChannel out = fos.getChannel() ;
		
		in.transferTo(0, in.size(), out) ;
		
		out.close() ;
		in.close() ;
		fos.close() ;
		fis.close() ;
		
	}
	
	
	public static String file_name_change(String file_path){
		String[] arr = file_path.split("\\.") ;
		String out_path = arr[0] + "_MID." + arr[1] ;
		return out_path ;
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
		try {
//			br = new BufferedReader(new InputStreamReader(new FileInputStream(file_path), codeString(file_path))) ;
			br = new BufferedReader(new InputStreamReader(new FileInputStream(file_path), "GBK")) ;
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out_path), "UTF-8")) ;
			while ((line = br.readLine()) != null) {
//				file += line + "\n" ;
				line = line.replace("\r", "").trim().replaceAll("\\s*!\\s*", "!") ;
				bw.write(line + "\n") ;
			}
			bw.close() ;
			br.close() ;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return out_path ;
	}
	
	public void create_directory(String path){
		File file = new File(path) ;
		if(!file.exists() && !file.isDirectory()){
			System.out.println(path + ":目录不存在 \n创建");
			file.mkdir() ;
		}else{
			System.out.println(path + ":目录存在");
		}
	}
	
	/**
	 * delete file
	 * */
	public boolean file_delete(String file_path){
		boolean flag = false ;
		File delete_file = new File(file_path) ;
		
		if(!delete_file.exists()){
			flag = false ;
		}else{
			delete_file.delete() ;
			flag = true ;
		}
		
		return flag ;
	}
	
	public Map<String, String> partition_table_funct(String file_path) throws Exception{
		Map<String, String> map = new HashMap<String, String>() ;
		File file = new File(file_path) ;
		BufferedReader br = new BufferedReader(new FileReader(file)) ;
		String line = "" ;
		String[] table_par_fun ;
		while( ( line = br.readLine() ) != null ){
			line = line.trim() ;
			table_par_fun = line.split(" ") ;
			if(table_par_fun.length < 2){
				continue ;
			}else{
				map.put(table_par_fun[0], table_par_fun[1]) ;
				System.out.println(line);
			}
		}
		br.close() ;
		return map ;
	}
	
	public static String codeString(String file_path) throws Exception{
		BufferedInputStream  br = new BufferedInputStream(new FileInputStream(file_path)) ;
		int p = (br.read() << 8) + br.read() ;
		String code = null ;
		switch(p){
			case 0xefbb:  
	            code = "UTF-8";  
	            break;  
	        case 0xfffe:  
	            code = "Unicode";  
	            break;  
	        case 0xfeff:  
	            code = "UTF-16BE";  
	            break;  
	        default:  
	            code = "GBK";  
		}
		br.close() ;
		return code ;
	}
	
	
	public static void main(String[] args) {
		
		
	}
}
