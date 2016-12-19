package com.hiwan.dimp.incremental.myudf;

import java.text.SimpleDateFormat;

import org.apache.hadoop.hive.ql.exec.UDF;

public final class HalfYearPartition extends UDF { //partition_halfyear

	public String evaluate(String date){
		String result;
		result = "";
		try {
			if( date == null ){
				result = "default" ;
			}else{
				date = date.trim() ;
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") ;
				if( date.length() == 8){
					SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd") ;
					date = sdf.format(sdf1.parse(date)) ;
				}
				String[] arr = date.split(" ")[0].split("-") ;
				if(arr.length < 3){
					result = "default" ;
				}else{
					String year = arr[0] ;
					String month = arr[1] ;
					String day = arr[2] ;
					if("2007".compareTo(year) > 0){
						result = "P200706" ;
					}else{
						if("01".equals(month) && "01".equals(day)){
							result = "P" + (Integer.parseInt(year)-1) + "12" ;
						}else if("07".compareTo(month) > 0 || ("07".equals(month) && "01".equals(day))){
							result = "P" + year + "06" ;
						}else if("06".compareTo(month) < 0){
							result = "P" + year + "12" ;
						}else{
							result = "default" ;
						}
					}
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			result = "default" ;
		}
		
		return result ;
	}
	
}
