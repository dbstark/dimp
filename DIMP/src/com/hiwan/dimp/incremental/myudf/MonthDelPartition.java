package com.hiwan.dimp.incremental.myudf;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.hive.ql.exec.UDF;

public final class MonthDelPartition extends UDF {

	//特殊月分区:P200912_DEL/P200912
	public String evaluate(String data){
		data = data.trim() ;
		String result = "";
		SimpleDateFormat data_sdf = new SimpleDateFormat("yyyyMMdd") ;
		SimpleDateFormat format_sdf = new SimpleDateFormat("yyyy-MM-dd") ;
		int length = data.length() ;
		if(data.indexOf("-") > -1){
			if(length == 8 || length == 9 || length == 10){
				data_sdf = new SimpleDateFormat("yyyy-M-d") ;
			}else if(length == 13 || length == 14 || length == 15){
				data_sdf = new SimpleDateFormat("yyyy-M-d HH:mm") ;
			}else if(length == 16 || length == 17 || length == 18){
				data_sdf = new SimpleDateFormat("yyyy-M-d HH:mm:ss") ;
			}
		}else{
			if(length == 8){
				data_sdf = new SimpleDateFormat("yyyyMMdd") ;
			}else if(length == 14){
				data_sdf = new SimpleDateFormat("yyyyMMddHHmmss") ;
			}
		}
		
		try {
			String date = format_sdf.format(data_sdf.parse(data)) ;
			String[] time_arr = date.split("-") ;
			String day = time_arr[2] ;
			if("01".equals(day)){
				Calendar c = Calendar.getInstance() ;
				c.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(data)) ;
				c.set(Calendar.MONTH, c.get(Calendar.MONTH) -1 ) ;
				result = "P" + new SimpleDateFormat("yyyyMM").format(c.getTime());
			}else{
				result = "P" + time_arr[0] + time_arr[1] + "_DEL";
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			result = "default" ;
		}
		return result ;
	}
	
}
