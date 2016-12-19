package com.hiwan.dimp.incremental.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import com.hiwan.dimp.incremental.bean.AugmentInfo;

public class HdfsFileSystem {

	Configuration conf ;
	FileSystem fs ;
	DistributedFileSystem hdfs ;
	int buffer_size = 10*1024 ;
	
	URI uri ;
	
	public HdfsFileSystem(){
		conf = new Configuration() ;
//		uri = URI.create("hdfs://21.144.1.3:8020/") ;
//		uri = URI.create("hdfs://hdfs_host:8020/") ;
		try {
//			fs = FileSystem.get(uri , conf) ;
		    //new FileSystem(conf);
			fs = FileSystem.get(conf) ;
			hdfs = (DistributedFileSystem) fs ;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void create_hfs_dir(String hdfs_path){
		
		try {
			fs.mkdirs(new Path(hdfs_path)) ;
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public boolean file_hfs_exists(String hdfs_path){
		boolean result = false ;
		try {
			result =  fs.exists(new Path(hdfs_path)) ;
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}
	
	public void put_file_to_hdfs(String local_path , String put_shell , AugmentInfo aug_info) throws Exception {
		File file = new File(local_path) ;
//		String target_path = "hdfs://21.144.1.3:8020" + put_shell.split("data_file_path")[1].trim() ;
		String target_path = put_shell.split("data_file_path")[1].trim() ;
		String type = aug_info.getTable_type() ;
		String is_partition = aug_info.getIs_partition() ;
		if( !("信息表".equals(type)) && !("1".equals(is_partition)) ){
			target_path = target_path + "/" + "hive" ;
		}
		Path path = new Path(target_path + "/" +  file.getName()) ;
		System.out.println("目标路径:"+target_path + "/" +  file.getName());
		InputStream is = null ;
		FSDataOutputStream os = null ;
		is = new BufferedInputStream(new FileInputStream(file) , buffer_size) ;
		os = fs.create(path, true, 0);
		byte[] buf = new byte[buffer_size] ;
		int readbytes = 0 ;
		while ((readbytes = is.read(buf)) > 0) {
			os.write(buf, 0, readbytes); 
		}
		os.close(); 
		is.close();
	}
	
	
	public void put_file_to_hdfs2(String local_path , String put_shell) throws Exception {
		File file = new File(local_path) ;
		String target_path = put_shell ;
		Path path = new Path(target_path + "/" +  file.getName()) ;
		System.out.println("目标路径:"+target_path + "/" +  file.getName());
		InputStream is = null ;
		FSDataOutputStream os = null ;
		is = new BufferedInputStream(new FileInputStream(file) , buffer_size) ;
//		System.out.println("FileSystem:" + fs);
		os = fs.create(path, true, 0);
		byte[] buf = new byte[buffer_size] ;
		int readbytes = 0 ;
		while ((readbytes = is.read(buf)) > 0) {
			os.write(buf, 0, readbytes); 
		}
		os.close(); 
		is.close();
	}
	
	public long put_file_to_hdfs3(String local_path , String put_shell) throws Exception {
		File file = new File(local_path) ;
		String target_path = put_shell  ;
		fs.mkdirs(new Path(target_path)) ;
		Path path = new Path(target_path + "/" +  file.getName()) ;
		InputStream is = null ;
		FSDataOutputStream os = null ;
		is = new BufferedInputStream(new FileInputStream(file) , buffer_size) ;
		os = fs.create(path, true, 0);
		byte[] buf = new byte[buffer_size] ;
		int readbytes = 0 ;
		while ((readbytes = is.read(buf)) > 0) {
			os.write(buf, 0, readbytes); 
		}
		os.close(); 
		is.close();
		FileStatus fstatus = fs.getFileStatus(path) ;
		return fstatus.getLen() ;
	}
	
	public void colse(){
		/*
		try {
			if(fs != null){
				fs.close();
			}
			if(hdfs != null){
				hdfs.close(); 
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
	}
	
	
	public static void main(String[] args) {

	}
	
	
}
