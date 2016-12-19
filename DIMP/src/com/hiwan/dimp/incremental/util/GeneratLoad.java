package com.hiwan.dimp.incremental.util;

import com.hiwan.dimp.incremental.bean.AugmentInfo;

public class GeneratLoad {

	/**
	 * 用于生成load data to hdfs的脚本
	 * */
	public static String load_sql(AugmentInfo aug_info){
		StringBuilder sql = new StringBuilder("   ") ;
		
//		String hive_path = "/inceptorsql1/user/hive/warehouse/" ;
		/**
		 * 判断是否为信息表：
		 * 是：数据load入中间表
		 * 否：数据load入目标表
		 * */
		String table_type = aug_info.getTable_type() ;
		String target_table_name = "" ;
		if("信息表".equals(table_type) ){
//			target_table_name = aug_info.getMid_table_name() ;
			target_table_name = aug_info.getTemp_table_name() ;
		}else if("1".equals(aug_info.getIs_partition())){
			target_table_name = aug_info.getMid_table_name() ;
		}else{
			target_table_name = aug_info.getTable_name() ;
		}
		sql.append("  ") ;
		sql.append("  hadoop fs -put ") ;
		sql.append("  data_file_path ") ;
		sql.append("  /inceptorsql1/user/hive/warehouse/").append(target_table_name) ;
		aug_info.setAddfile_hdfs_script(sql.toString()) ;
		return sql.toString() ;
	}
	
}
