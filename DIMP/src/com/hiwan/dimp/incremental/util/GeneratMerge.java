package com.hiwan.dimp.incremental.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.hiwan.dimp.incremental.bean.AugmentInfo;

public class GeneratMerge {

	/**
	 * 用于生成信息表:中间表到目标表的数据导入sql
	 * @throws Exception 
	 * */
	public static String sql_generat(AugmentInfo aug_info) throws Exception{
		StringBuilder sql = new StringBuilder("   ") ;
		
		List<String> key_column_list = new ArrayList<String>() ; //主键
		List<String> other_column_list = new ArrayList<String>() ; // update column
		List<String> all_column_list = new ArrayList<String>() ; //insert column

		String table_name = aug_info.getTable_name() ;
		String is_partition = aug_info.getIs_partition() ;
		String mid_table_name = aug_info.getMid_table_name() ;
		String primary_key = aug_info.getPrimary_key() ; //对主键进行切分
		String create_script_hive = aug_info.getCreate_script_hive() ;
		
		Map<String, String> colInfo = new LinkedHashMap<String, String>();
//		String str = allRowListSql(table_name);
		/**
		 * 根据表名称,是否为分区表,中间表名称等生成merge语句,只有信息表需要生成merge语句
		 * */
		/**
		 * 首先获取主键的list
		 * */
		for(String key : primary_key.split(",")){
			key_column_list.add(key.toUpperCase()) ;
		}
		
		/**
		 * 拼装需要的列信息
		 * */
		create_script_hive = create_script_hive.substring(create_script_hive.indexOf("(")+1, create_script_hive.indexOf(")")) ;
		for(String column_type : create_script_hive.split(",")){
			String column = column_type.split(" ")[0].trim().toUpperCase() ;
			all_column_list.add(column) ;
			if(!key_column_list.contains(column)){
				other_column_list.add(column) ;
			}
		}
		
		/**
		 * 根据目标表,中间表,主键列,非主键列等信息,拼装merge语句
		 * 模版：
		 * MERGE INTO t98_indpty_prod_stat_orc_4_partition 
		 * PARTITION (summ_date='2012') t2 
		 * USING t98_indpty_prod_stat_orc_3_group_limit t3 
		 * ON 
		 * (t2.STAT_ORG_ID=t3.STAT_ORG_ID and t2.BIZ_TYPE_CD = t3.BIZ_TYPE_CD and t2.CONFORM_INDPARTY_ID=t3.CONFORM_INDPARTY_ID and t2.PRODUCT_ID=t3.PRODUCT_ID and t2.CURRENCY_CD=t3.CURRENCY_CD) 
		 * WHEN MATCHED THEN UPDATE SET acct_num = t3.acct_num 
		 * WHEN NOT MATCHED THEN INSERT (stat_org_id,biz_type_cd,conform_indparty_id,product_id,currency_cd,acct_num,bal_amt,quot ,market_value,bad_loan_bal_amt,mon_daily_avg_bal_amt,qua_daily_avg_bal_amt,yr_daily_avg_bal_amt,province_cd ,txdate,p_date) 
		 * VALUES (t3.stat_org_id,t3.biz_type_cd,t3.conform_indparty_id,t3.product_id,t3.currency_cd,t3.acct_num,t3.bal_amt,t3.quot ,t3.market_value,t3.bad_loan_bal_amt,t3.mon_daily_avg_bal_amt,t3.qua_daily_avg_bal_amt,t3.yr_daily_avg_bal_amt,t3.province_cd ,t3.txdate,t3.p_date);
		 * */
		sql.append(" merge into ").append(table_name) ;
		sql.append(" partition_value ").append(" target ") ; //分区表：替换为partition(value)  非分区表：替换为空格
		sql.append(" using ").append(mid_table_name).append(" mid ") ;
		sql.append(" on ") ;
		sql.append(" ( 1 = 1 ") ;
		for(String key : key_column_list){
			sql.append(" and target.").append(key).append(" = ").append("mid.").append(key) ;
		}
		sql.append(" ) ") ;
		/**
		 * 判断除了出件是否还有其余的列
		 * */
		if(other_column_list.size() > 0){
			sql.append(" when matched then update set ") ;
			int i = 0 ;
			for(String column : other_column_list){
				if(i==0){
					sql.append(" ").append(column).append("=").append("mid.").append(column).append(" ") ;
					i = i + 1;
				}else{
					sql.append(" , ").append(column).append("=").append("mid.").append(column).append(" ") ;
				}
			}
		}
		sql.append(" when not matched then ") ;
		int j = 0 ;
		StringBuilder target_column = new StringBuilder(" insert (  ") ;
		StringBuilder value_column = new StringBuilder(" values ( ") ;
		for(String column : all_column_list){
			if(j == 0){
				target_column.append(" ").append(column) ;
				value_column.append("mid.").append(column) ;
//				sql.append(" ").append(column).append("=").append("mid.").append(column).append(" ") ;
				j++ ;
			}else{
				target_column.append(" , ").append(column) ;
				value_column.append(" , ").append("mid.").append(column)  ;
//				sql.append(" , ").append(column).append("=").append("mid.").append(column).append(" ") ;
			}
		}
		sql.append(target_column).append(" ) ").append(value_column).append(" ) ") ;
//		sql.append(" ) ") ; 
		
		aug_info.setImport_script(sql.toString()) ;
		return sql.toString() ;
	}
	
	public static String allRowListSql(String sql) {
		StringBuilder sb = new StringBuilder("   ");
		sb.append("   ");

		sb.append("   select *  ");
		sb.append("    from (select a.*, rownum row_num ");
		sb.append("     from ( ").append(sql).append(" ) a) b  ");

		return sb.toString();
	}
	
	public static void main(String[] args){
		String s = "abcdefgabcdefg" ;
		
		System.out.println(s.indexOf("b"));
		System.out.println(s.indexOf("d"));
	}
	
	
}
