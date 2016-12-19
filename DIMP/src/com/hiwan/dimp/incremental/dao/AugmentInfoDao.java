package com.hiwan.dimp.incremental.dao;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.beanutils.BeanUtils;

import com.hiwan.dimp.dao.PageToolMysql;
import com.hiwan.dimp.incremental.bean.AugmentInfo;

public class AugmentInfoDao {

	/**
	 * 获取增量导入参数
	 * 拼装sql
	 * @throws InvocationTargetException 
	 * @throws IllegalAccessException 
	 * */
	public List<AugmentInfo> getAugInfoList( List<Map<String, Object>> list ) throws IllegalAccessException, InvocationTargetException{
		List<AugmentInfo> aug_info_list = new ArrayList<AugmentInfo>() ;
		for(Map<String, Object> map : list){
			AugmentInfo aug_info = new AugmentInfo() ;
			BeanUtils.populate(aug_info, map) ;
			aug_info_list.add(aug_info) ;
		}
		return aug_info_list ;
	}
	
	public Map<String , AugmentInfo> getAugInfoMap(List<String> list) throws SQLException, IllegalAccessException, InvocationTargetException{
		//拼装查询sql,关联查询
		Map<String, AugmentInfo> aug_info_map = new HashMap<String, AugmentInfo>() ;
		List<Map<String, Object>> aug_info_map_list = PageToolMysql.allRowList(this.getAugInfoSql(list)) ;
		List<AugmentInfo> aug_info_list = this.getAugInfoList(aug_info_map_list) ;
		for(AugmentInfo aug_info : aug_info_list){
			aug_info_map.put(aug_info.getTable_name(), aug_info) ;
		}
		return aug_info_map ;
	}
	
	public Map<String , AugmentInfo> getAugInfoMap2(List<String> list) throws SQLException, IllegalAccessException, InvocationTargetException{
		//拼装查询sql,关联查询
		/*Map<String, AugmentInfo> aug_info_map = new TreeMap<String, AugmentInfo>(new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				// TODO Auto-generated method stub
				return o2.compareTo(o1);
			}
		}) ;*/
		Map<String, AugmentInfo> aug_info_map = new TreeMap<String, AugmentInfo>() ;
		List<Map<String, Object>> aug_info_map_list = PageToolMysql.allRowList(this.update_base_info(list)) ;
		List<AugmentInfo> aug_info_list = this.getAugInfoList(aug_info_map_list) ;
		for(AugmentInfo aug_info : aug_info_list){
			aug_info_map.put(aug_info.getTable_id(), aug_info) ;
		}
		
		return aug_info_map ;
	}
	
	/**
	 * 拼装查询sql
	 * */
	public String getAugInfoSql(List<String> list){
		StringBuilder sql = new StringBuilder("   ") ;
		sql.append(" select ") ;
		sql.append("       aug.id aug_id , ") ;
		sql.append("       aug.table_id table_id, ") ;
		
		sql.append("       meta.table_name table_name, ") ;
		sql.append("       meta.table_type table_type, ") ;
		sql.append("       meta.target_table target_table, ") ;
		sql.append("       meta.is_partition is_partition, ") ;
		sql.append("       meta.partition_field partition_field, ") ;
		sql.append("       meta.primary_key primary_key, ") ;
		sql.append("       meta.create_script_hive create_script_hive, ") ;
		
		sql.append("       aug.partition_fun partition_fun, ") ;
		sql.append("       aug.mid_table_name mid_table_name, ") ;
		sql.append("       aug.temp_table_name temp_table_name, ") ;
		sql.append("       aug.create_mid_sql create_mid_sql, ") ;
		sql.append("       aug.create_temp_sql create_temp_sql, ") ;
		sql.append("       aug.temp_to_mid temp_to_mid, ") ;
		sql.append("       aug.addfile_hdfs_script addfile_hdfs_script, ") ;
		sql.append("       aug.duplicate_removal duplicate_removal, ") ;
		sql.append("       aug.import_script import_script, ") ;
		sql.append("       aug.lst_modify_date lst_modify_date, ") ;
		sql.append("       aug.status status ") ;
		sql.append("  from ") ;
		
		sql.append("  ( select * from  tb_table_augment where 1=1 ") ;
		sql.append("  )aug ") ;
		sql.append("  left join ") ;
		sql.append("  ( select * from tb_table_metadata where 1=1 ") ;
		sql.append("  ) meta ") ;
		sql.append("  on aug.table_id = meta.id ") ;
		if(list.size() > 0){
			String sql1 = sql.toString() ;
			sql = new StringBuilder( " select * from ( " ) ;
			sql.append(sql1) ;
			sql.append(" ) t where " ) ;
			int i = 0 ;
			for(String table_name : list){
				if( i==0 ){
					sql.append(" t.table_name = '" + table_name + "' ") ;
					i++ ;
				}else{
					sql.append(" or t.table_name = '" + table_name + "' ") ;
				}
			}
		}
		return sql.toString() ;
	}
	
	
	public String update_base_info(List<String> list){
		
		StringBuilder sql = new StringBuilder("  ") ;
		
		/**
		 * select meta.id table_id  , meta.status status from
		 * ( select * from tb_table_metadata where 1=1 ) meta 
		 * left join 
		 * ( select * from  tb_table_augment where 1=1 ) aug 
		 * on aug.table_id = meta.id
		 * 
		 * */
		
		sql.append("select ") ;
		sql.append("  meta.id table_id  , ") ;
		sql.append("  meta.status status , ") ;
		sql.append("  meta.table_name table_name ,") ;
		sql.append("  meta.is_partition is_partition ,") ;
		sql.append("  meta.table_type table_type ") ;
		sql.append("  from ") ;
		sql.append("  ( select * from tb_table_metadata where 1=1 and ( status =0 or status = 1 ) ") ;
		if(list.size() > 0){
			sql.append( " and ( " ) ;
			int i = 0 ;
			for(String table_name : list){
				if( i==0 ){
					sql.append(" table_name = '" + table_name + "' ") ;
					i++ ;
				}else{
					sql.append(" or table_name = '" + table_name + "' ") ;
				}
			}
			sql.append(" ) " ) ;
		}
		sql.append("  ) meta ") ;
		return sql.toString() ;
	}
	
	public static void main(String[] args) {
		System.out.println(new AugmentInfoDao().update_base_info(new ArrayList<String>()) );
	}
}
