package com.hiwan.dimp.databak.dao;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import com.hiwan.dimp.incremental.bean.SourceFileBean;
import com.hiwan.dimp.incremental.util.HdfsFileSystem;
import com.hiwan.dimp.incremental.util.HiveConnection2;

public class DataLoadBakTable {

	public void data_load(Map<String, Integer> map, String table_name,String cim_job_name , 
			List<SourceFileBean> source_bean_list, HdfsFileSystem hfs,
			HiveConnection2 hive_conn) throws Exception {
		
		Integer day_num = map.get(table_name) ;
		if( day_num == null ){
			day_num = 40 ;
		}
		//对source_bean_list进行循环,进行删除和新增的操作
		table_name = table_name + "_hisdaybak" ;
		table_name = table_name.toLowerCase() ;
		String mid_file_path = null ;
		String file_date = null ;
		String del_par = null ;
		String table_hdfs_path = hive_conn.table_hdfs_path(table_name) ;
		for(SourceFileBean sfb : source_bean_list){
			file_date = sfb.getFile_date() ;
			del_par = date_change(day_num, file_date) ;
			mid_file_path = sfb.getMid_source_path() ;
			//删除日期转换后,超出天数的分区
			hive_conn.execute3(" alter table " + table_name + " drop if exists partition(p_date='" + del_par + "_" + cim_job_name + "')") ;
			//创建文件日期的分区
			hive_conn.execute3(" alter table " + table_name + " add if not exists partition(p_date='" + file_date + "_" + cim_job_name + "')") ;
			//把数据添加到对应的分区之下
			hfs.put_file_to_hdfs2(mid_file_path, table_hdfs_path + "/p_date=" + file_date + "_" + cim_job_name) ;
		}
	}
	
	
	public String date_change(int day_num , String file_date){
		String date = null ;
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd") ;
			Calendar c = Calendar.getInstance() ;
			c.setTime(sdf.parse(file_date)) ;
			c.set(Calendar.DAY_OF_YEAR, c.get(Calendar.DAY_OF_YEAR) -day_num ) ;
			
			date = sdf.format(c.getTime()) ;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return date ;
	}
	
	
	public static void main(String[] args) {
		
		DataLoadBakTable d = new DataLoadBakTable() ;
		System.out.println(d.date_change(40, "20151102"));
	
	}
	
}
