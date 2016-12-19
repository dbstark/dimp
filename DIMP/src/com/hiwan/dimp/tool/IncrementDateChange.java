package com.hiwan.dimp.tool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import com.hiwan.dimp.db.DBAccess;
import com.hiwan.dimp.incremental.bean.IncrementDate;
import com.hiwan.dimp.incremental.dao.IncrementDateDao;
import com.hiwan.dimp.incremental.util.HbaseConnection;

public class IncrementDateChange {

	Connection conn = null;
	HbaseConnection hbase_conn = null ;
	public IncrementDateChange(){
		conn = DBAccess.getConnection_ds_mysql(); 
		hbase_conn = new HbaseConnection() ;
	}
	
	//获取etl_job中的数据
	public void date_from_etljob_to_incdate(){
		List<IncrementDate> list = new ArrayList<IncrementDate>() ;
		IncrementDateDao inc_date_dao = new IncrementDateDao() ;
		String sql = "select * from etl_job" ;
		PreparedStatement psmt = null; 
		PreparedStatement psmt1 = null; 
		ResultSet rs = null ;
		IncrementDate inc_date = null ;
		SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy") ;
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd") ;
		try {
			psmt = conn.prepareStatement(sql) ;
			psmt1 = conn.prepareStatement("") ;
			rs = psmt.executeQuery() ;
			while(rs.next()){
				String job_table = rs.getString(1).trim() ;
				if(job_table.contains("#")){
//					String job_frequency = rs.getString(2) ;
					String job_status = rs.getString(6).trim() ;
					String date = rs.getString(7).trim() ;
					date = sdf1.format(sdf.parse(date)) ;
					if("F".equals(job_status)){
						date = day_change(date) ;
					}
					String[] job_table_arr = job_table.split("#") ;
					inc_date = new IncrementDate(0, job_table_arr[1], job_table_arr[0], new Timestamp(System.currentTimeMillis()), date) ;
					inc_date_dao.insert_increment_date(inc_date) ;
					hbase_conn.putDownloaded(job_table_arr[1], date, job_table_arr[0]) ;
				}else{
					continue ;
				}
			}
			hbase_conn.putAll() ;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String day_change(String date)  {
		String result = "";
		try {
			date = date.trim() ;
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd") ;
			Calendar c = Calendar.getInstance() ;
			c.setTime(sdf.parse(date)) ;
			int day=c.get(Calendar.DATE); 
			c.set(Calendar.DATE,day-1);
			result = sdf.format(c.getTime())  ;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result ;
	}
	
	public String month_change(String date)  {
		String result = "";
		try {
			date = date.trim() ;
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd") ;
			Calendar c = Calendar.getInstance() ;
			c.setTime(sdf.parse(date)) ;
			int day=c.get(Calendar.DATE); 
			c.set(Calendar.DATE,day-1);
			result = sdf.format(c.getTime())  ;
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result ;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		IncrementDateChange inc_date_cha = new IncrementDateChange() ;
		inc_date_cha.date_from_etljob_to_incdate() ;
	}

}
