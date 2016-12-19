package com.hiwan.dimp.incremental.util;

import com.hiwan.dimp.incremental.bean.AugmentInfo;

public class MidTableCreateSQL {

	public static String create_midtable_sql(AugmentInfo aug_info){
		StringBuilder sql = new StringBuilder("  ") ;
		String create_sql = aug_info.getCreate_script_hive() ;
//		String table_name = aug_info.getTable_name() ;
		sql.append(" create table if not exists " ) ;
		sql.append(aug_info.getMid_table_name() + " ") ;
		sql.append(create_sql.substring(create_sql.indexOf("("), create_sql.indexOf(")") + 1)).append(" ") ;
		sql.append(" ").append(" row format delimited fields terminated by '!' stored as textfile ").append(" ") ;
		return sql.toString() ;
	}
	
	public static String create_temptable_sql(AugmentInfo aug_info){
		StringBuilder sql = new StringBuilder("  ") ;
		String create_sql = aug_info.getCreate_script_hive() ;
//		String table_name = aug_info.getTable_name() ;
		sql.append(" create table if not exists " ) ;
		sql.append(aug_info.getTemp_table_name() + " ") ;  //换成零时表名称即可
		sql.append(create_sql.substring(create_sql.indexOf("("), create_sql.indexOf(")") + 1)).append(" ") ;
		sql.append(" ").append(" row format delimited fields terminated by '!' stored as textfile ").append(" ") ;
		return sql.toString() ;
	}
	
	public static String temp_to_mid_sql(AugmentInfo aug_info){
		//insert overwrite table mid_table_name select * 
		//from (select * , row_number() over (partition by CARD_NO , SUB_ACC  ) num from CARD_OC_SUBACC_CLS) t where t.num = 1 
		StringBuilder sql = new StringBuilder(" ") ;
		sql.append(" insert overwrite table ") ;
		sql.append(aug_info.getMid_table_name()) ;
		sql.append(" select ") ;
		String create_sql = aug_info.getCreate_script_hive() ;
		String[] column_arr = create_sql.substring(create_sql.indexOf("(") + 1, create_sql.indexOf(")") ).split(",") ;
		
		for(int i = 0 ; i < column_arr.length ; i++){
			if(i==0){
				sql.append(column_arr[i].trim().split(" ")[0]) ;
			}else{
				sql.append(" , ") ;
				sql.append(column_arr[i].trim().split(" ")[0]) ;
			}
		}
		sql.append(" from ( select * , row_number() over (partition by " ) ;
		sql.append(aug_info.getPrimary_key() ) ;
		sql.append(" ) num from ") ;
		sql.append(aug_info.getTemp_table_name()) ;
		sql.append(" ) t where t.num = 1 ") ;
		// from (select *, row_number() over (partition by primary_key) num from temp_table_name) t where t.num = 1;
		return sql.toString() ;
	}
	
	public static void main(String[] args) {
		
//		String hive_sql = " create table TCS_PARTY_AGMT_RELA_H("
//				+ "INT_ORG_ID string,AGMT_NUM string,AGMT_MODIFIER_NUM double,PARTY_ID string,PARTY_AGMT_RELA_CD string,START_DT string,END_DT string,PROVINCE_CD double"
//				+ ") partitioned by (p_date string) "
//				+ "row format delimited fields terminated by '!' stored as textfile " ;
//		System.out.println(hive_sql.substring(hive_sql.indexOf("("), hive_sql.indexOf(")") + 1) + " row format delimited fields terminated by '!' stored as textfile ");
		System.out.println("   a b c  ".trim().split(" ").length);
	}
	
}
