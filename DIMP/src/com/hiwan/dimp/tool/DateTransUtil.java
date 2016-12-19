package com.hiwan.dimp.tool;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateTransUtil {
	
	SimpleDateFormat sdf  ;
	String ps_date  ;
	int ps_num ;
	Calendar c ;
	
	public DateTransUtil(){
		sdf = new SimpleDateFormat("yyyyMMdd") ;
		ps_date = "18991231" ;
		ps_num = 693960 ;
		try {
			c = Calendar.getInstance() ;
			c.setTime(sdf.parse(ps_date)) ;
			c.set(Calendar.HOUR_OF_DAY, 0);
			c.set(Calendar.MINUTE, 0);
			c.set(Calendar.SECOND, 0);
			c.set(Calendar.MILLISECOND, 0);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public long date_to_num(String date) throws ParseException{
		Calendar now_c = Calendar.getInstance() ;
		now_c.setTime(sdf.parse(date)) ;
		now_c.set(Calendar.HOUR_OF_DAY, 0);
		now_c.set(Calendar.MINUTE, 0);
		now_c.set(Calendar.SECOND, 0);
		now_c.set(Calendar.MILLISECOND, 0);
		long dayDiff =(now_c.getTimeInMillis()-c.getTimeInMillis())/(1000*60*60*24);
		return dayDiff ;
	}
	
	
	
	public String num_to_date(long num) throws ParseException{
		long date_s = sdf.parse(ps_date).getTime() ;
		date_s = date_s + num * 24L * 60L * 60L * 1000L ;
		String now_date = sdf.format(new Date(date_s)) ;
		return now_date ;
	}
	
	/**
	 * @param args
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws ParseException {
		// TODO Auto-generated method stub
		DateTransUtil dtu = new DateTransUtil() ;
//		System.out.println(dtu.date_to_num("20150714")) ;
		System.out.println(dtu.num_to_date(40237L));
		
	}

}
