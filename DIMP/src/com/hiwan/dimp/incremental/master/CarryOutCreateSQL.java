package com.hiwan.dimp.incremental.master;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.hiwan.dimp.incremental.bean.AugmentInfo;
import com.hiwan.dimp.incremental.dao.AugmentInfoDao;
import com.hiwan.dimp.incremental.util.HiveConnection2;

public class CarryOutCreateSQL {

	public void carry_out_sql( List<String> list) throws Exception{
		/**
		 * 获取数据库表的所有数据
		 * 执行创建零时表和目标表的脚本
		 * */
		AugmentInfoDao aug_info_dao = new AugmentInfoDao() ;
		Map<String, AugmentInfo> aug_info_map = aug_info_dao.getAugInfoMap(list) ;
		HiveConnection2 hive_conn = new HiveConnection2() ;
		AugmentInfo aug_info = null ;
		for(Entry<String, AugmentInfo> entry : aug_info_map.entrySet()){
			aug_info = entry.getValue() ;
			if("信息表".equals(aug_info.getTable_type())){
//				hive_conn.execute("drop table if exists " + aug_info.getMid_table_name()) ;
//				hive_conn.execute("drop table if exists " + aug_info.getTemp_table_name()) ;
				hive_conn.execute3(aug_info.getCreate_mid_sql()) ;
				hive_conn.execute3(aug_info.getCreate_temp_sql()) ;
			}else if("1".equals(aug_info.getIs_partition())){
//				hive_conn.execute("drop table if exists " + aug_info.getMid_table_name()) ;
				hive_conn.execute3(aug_info.getCreate_mid_sql()) ;
			}
		}
		hive_conn.close() ;
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			List<String> list = new ArrayList<String>() ;
			if( args!= null && args.length > 0){
				for(String table_name : args[0].split(",")){
					list.add(table_name) ;
				}
			}
			new CarryOutCreateSQL().carry_out_sql(list) ;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
