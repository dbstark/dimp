package com.hiwan.dimp.tool;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.hiwan.dimp.incremental.util.HdfsFileSystem;

public class CodeFileToHDFS {

	public void file_to_hdfs(String input_dir , String hdfs_dir , String date) throws Exception{
		
		hdfs_dir = hdfs_dir + "/" + date ;
		HdfsFileSystem hfs = new HdfsFileSystem() ;
		Map<String, String> map = new HashMap<String, String>() ;
		map = list_code_files(input_dir, map, date) ;
		for(Entry<String, String> entry : map.entrySet()){
			hfs.put_file_to_hdfs3(entry.getValue(), hdfs_dir) ;
			new File(entry.getKey()).delete() ;
		}
		
	}
	
	public Map<String, String> list_code_files(String input_dir , Map<String, String> map , String date) throws Exception{
		File file = new File(input_dir) ;
		File[] arr_file = file.listFiles() ;
		SimpleDateFormat sdf =  new SimpleDateFormat("yyyyMM") ;
		SimpleDateFormat sdf1 =  new SimpleDateFormat("yyyyMMdd") ;
		String file_date = null ;
		String[] f_arr = null ;
		for(File f : arr_file){
			String file_name = f.getName() ;
			f_arr = file_name.split("_") ;
			file_date = f_arr[f_arr.length -3] ;
			file_date = sdf.format(sdf1.parse(file_date)) ;
			if(date.equals(file_date)){
				map.put(file_name, f.getPath()) ;
			}
		}
		return map;
	}
	
	
	public void put_file_to_hbfs( HdfsFileSystem hfs  , String input_file,String hdfs_file,String date){
		//获取本地文件列表
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM") ;
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMdd") ;
		File local = new File(input_file) ;
		File[] ls_file = local.listFiles() ;
		String name = null ;
		String[] f_arr = null ;
		String file_date = null ;
		if(ls_file == null || ls_file.length == 0){
			System.out.println("没有文件需要加载");
		}else {
			for(File f : ls_file){
				if(f.isDirectory()){
					put_file_to_hbfs(hfs ,f.getPath(), hdfs_file,date) ;
				}else if (f.isFile()){
					if (System.currentTimeMillis() - f.lastModified() > 10*60 * 1000) {
						name = f.getName() ;
						System.out.println("文件名"+name);
						if(! name.endsWith(".dat.gz.D")){
							continue ;
						}
						f_arr = name.split("_") ;
						file_date = f_arr[f_arr.length -3] ;
						try {
							if(date.equals(sdf.format(sdf2.parse(file_date)))){
								String file_to_hdfs_path = hdfs_file + "/" + date + "/" + name ;
								/*String command = "hadoop fs -put "
										+ f.getPath() + " " 
										+ hdfs_file + "/" + date + "/";
								Process process = Runtime.getRuntime().exec(command);
								System.out.println(command);*/
								
								long hdfs_size = hfs.put_file_to_hdfs3(f.getPath(), hdfs_file + "/" + date) ;
								/*StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "ERROR","");
								errorGobbler.start(); 
								StreamGobbler outGobbler = new StreamGobbler(process.getInputStream(), "STDOUT","");
								outGobbler.start(); 
								process.waitFor(); */
								
								if(hfs.file_hfs_exists(file_to_hdfs_path) && f.length() == hdfs_size ){
									f.delete() ;
								}
							}
						} catch (Exception e) {
							// TODO Auto-generated catch block
//							e.printStackTrace();
						}
					}
				}
			}
		}
	}
	
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		/*if(args == null || args.length == 0){
			System.out.println("请输入需要备份的数据文件日期：如201601");
			System.exit(0) ;
		}*/
		String month_time = "" ;
		if(args == null || args.length == 0){
			Calendar c = Calendar.getInstance() ;
			c.setTime(new Date()) ;
			c.set(Calendar.MONTH, c.get(Calendar.MONTH) -1 ) ;
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM") ;
			month_time = sdf.format(c.getTime()) ;
		}else{
			month_time = args[0] ;
		}
		CodeFileToHDFS cfth = new CodeFileToHDFS() ;
		String input_dir = "/home/jshdata_increment/data/code" ;
		String hdfs_dir = "/code" ;
		HdfsFileSystem hfs = new HdfsFileSystem() ;
//		hfs.create_hfs_dir(hdfs_dir + "/" + args[0]) ;
		hfs.create_hfs_dir(hdfs_dir + "/" + month_time) ;
//		cfth.put_file_to_hbfs(hfs, input_dir, hdfs_dir,args[0]) ;
		cfth.put_file_to_hbfs(hfs, input_dir, hdfs_dir,month_time) ;
	}

}
