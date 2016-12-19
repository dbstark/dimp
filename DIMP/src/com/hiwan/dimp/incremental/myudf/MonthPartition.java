package com.hiwan.dimp.incremental.myudf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;

public final class MonthPartition extends UDF{

	public String evaluate(String data) {
		data = data.trim() ;
		String result = "";
		SimpleDateFormat data_sdf = new SimpleDateFormat("yyyyMMdd") ;
		SimpleDateFormat format_sdf = new SimpleDateFormat("yyyyMM") ;
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
			data = format_sdf.format(data_sdf.parse(data)) ;
			result = "P" + data ;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			result = "default" ;
		}
		return result ;
	}
	
	
	public static void main(String[] args) throws ParseException {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-M-d HH:mm:ss") ;
		System.out.println(sdf.format(new Date()));
		
	}
}
