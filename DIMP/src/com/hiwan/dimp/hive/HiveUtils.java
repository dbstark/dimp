package com.hiwan.dimp.hive;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class HiveUtils {

	private HiveConnection hc = null ;
	private ResultSet rs = null ;
	private ResultSetMetaData rsmd = null ;

	public HiveUtils(){
		hc = new HiveConnection() ;
	}
	
	public void deleteTable( String tableName ){
		String sql = "drop table if exists " + tableName;
		hc.execute(sql) ;
	}
	public void truncateTable(String tableName){
		String sql="truncate table "+tableName;
		hc.execute(sql);
	}
	public void createLikeTable(String tableName,String postfix){
		String sql="create table "+tableName+postfix+ " like " +tableName;
		hc.execute(sql);
	}
	public void close(){
		if(rs!=null){
			try {
				rs.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if(hc!=null){
			hc.close();
		}
	}
	public void createTable( String tablename ){
		deleteTable(tablename) ;
		String sql = "create table " + tablename + " (key int, value string)  row format delimited fields terminated by '\t'";
		hc.execute(sql) ;
	}
	
	public void seleteTable( String tablename ) {
		try {
			String sql = "select * from " + tablename;
			rs = hc.execute(sql) ;
			rsmd = rs.getMetaData() ;
			int column = rsmd.getColumnCount() ;
			for( int i = 0 ; i < column ; i++ ){
				System.out.print(rsmd.getColumnName(i+1) + "\t");
			}
			System.out.println();
			while(rs.next()){
				for(int i = 0 ; i < column ; i++){
					 System.out.print(rs.getString(i+1) + "\t");   
				}
				System.out.println();   
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 统计Hive表中记录数
	 * @throws SQLException 
	 *  @time  :2015年7月10日 上午11:08:49
	 */
	public int rowCount(String tableName) throws SQLException{
		String sql = "select count(*)  from " + tableName;
		rs = hc.execute(sql) ;
		int rowCount=0;
		while(rs.next()){
			rowCount=rs.getInt(1);
			//System.out.println(rs.getString("records"));
		}
		
		rs.close();
		hc.close();
		return rowCount;
	}
	public static void main(String[] args) throws SQLException {
		int n=new HiveUtils().rowCount("TB_BILL_PAPER_DISCOUNT");
		
		System.out.println(n);
	}
}
