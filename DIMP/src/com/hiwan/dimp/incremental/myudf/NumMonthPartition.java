package com.hiwan.dimp.incremental.myudf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;

public final class NumMonthPartition extends UDF {

	//月分区情况2010-02-01 00:00:00
	public String month_partition_to_string(String date) {
		String result = "";
		try {
			date = date.trim() ;
			String[] time_arr = date.split(" ")[0].split("-") ;
			String day = time_arr[2] ;
			/*if("01".equals(day)){
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") ;
				Calendar c = Calendar.getInstance() ;
				c.setTime(sdf.parse(date)) ;
				c.set(Calendar.MONTH, c.get(Calendar.MONTH) -1 ) ;
				SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMM") ;
				result = "P" + sdf2.format(c.getTime()) ;
			}else{
				result = "P" + time_arr[0] + time_arr[1] ;
			}*/
			result = "P" + time_arr[0] + time_arr[1] ;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			result = "default" ;
		}
		return result ;
	}
	
	//把数字格式的日期转换为月分区
	public String evaluate(int date){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") ;
		Calendar c ;
		String result = "" ;
		String ps_date = "1899-12-31 00:00:00" ;
		try {
			c = Calendar.getInstance() ;
			c.setTime(sdf.parse(ps_date)) ;
			c.set(Calendar.HOUR_OF_DAY, 0);
			c.set(Calendar.MINUTE, 0);
			c.set(Calendar.SECOND, 0);
			c.set(Calendar.MILLISECOND, 0);
			long date_s = sdf.parse(ps_date).getTime() ;
			date_s = date_s + date * 24L * 60L * 60L * 1000L ;
			String now_date = sdf.format(new Date(date_s)) ;
			result = month_partition_to_string(now_date) ;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			result = "default" ;
		}
		return result ;
	}
	
	
	public String evaluate(double date){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") ;
		Calendar c ;
		String result = "" ;
		String ps_date = "1899-12-31 00:00:00" ;
		try {
			c = Calendar.getInstance() ;
			c.setTime(sdf.parse(ps_date)) ;
			c.set(Calendar.HOUR_OF_DAY, 0);
			c.set(Calendar.MINUTE, 0);
			c.set(Calendar.SECOND, 0);
			c.set(Calendar.MILLISECOND, 0);
			long date_s = sdf.parse(ps_date).getTime() ;
			date_s = date_s + (int)date * 24L * 60L * 60L * 1000L ;
			String now_date = sdf.format(new Date(date_s)) ;
			result = month_partition_to_string(now_date) ;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			result = "default" ;
		}
		return result ;
	}
	
}
