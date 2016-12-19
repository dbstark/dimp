package com.hiwan.dimp.incremental.master;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.sql.rowset.serial.SerialBlob;

import com.hiwan.dimp.db.DBAccess;
import com.hiwan.dimp.incremental.bean.AugmentInfo;
import com.hiwan.dimp.incremental.dao.AugmentInfoDao;
import com.hiwan.dimp.incremental.util.FileListUtil;
import com.hiwan.dimp.incremental.util.GeneratLoad;
import com.hiwan.dimp.incremental.util.GeneratMerge;
import com.hiwan.dimp.incremental.util.MidTableCreateSQL;

/**
 * 控制mysql数据库中数据更新
 * */
public class UpdateMaster {

	/**
	 * 获取数据,并且更新
	 * @throws InvocationTargetException 
	 * @throws IllegalAccessException 
	 * @throws SQLException 
	 * */
	/*Connection conn = null ;
	public UpdateMaster(){
		conn = DBAccess.getConnection_ds_mysql();
	}
	*/
	public void insert_augment(List<String> list) throws Exception {
		AugmentInfoDao aug_info_dao = new AugmentInfoDao() ;
//		Map<String, String> condition_map = new HashMap<String, String>() ;
		Map<String, AugmentInfo> aug_info_map = aug_info_dao.getAugInfoMap2(list) ;

		Connection conn = DBAccess.getConnection_ds_mysql();
		PreparedStatement psmt = null;
//		psmt = conn.prepareStatement("update tb_table_augment set Addfile_hdfs_scropt=?,Import_script=? where table_id=?");
//		psmt = conn.prepareStatement("insert into tb_table_augment(table_id,status,mid_table_name,lst_modify_date) values(?,?,?,?)");
		psmt = conn.prepareStatement("insert into tb_table_augment(table_id,status,mid_table_name,temp_table_name,lst_modify_date) values(?,?,?,?,?)");
		AugmentInfo aug_info ;
		int status ;
		List<String> list_table = new ArrayList<String>() ;
		list_table.add("T03_CARD") ;
		list_table.add("INN_DTL") ;
		list_table.add("CDM_DTL") ;
		list_table.add("AGT_JNL") ;
		list_table.add("RATETAX_DTL") ;
		list_table.add("PUB_JNL_OTH") ;
		for(Entry<String, AugmentInfo> entry : aug_info_map.entrySet()){
			aug_info = entry.getValue() ;
			psmt.setInt(1, Integer.parseInt(aug_info.getTable_id())) ;
			status = Integer.parseInt(aug_info.getStatus()) ;
			String table_name = aug_info.getTable_name() ;
			if("PK_FIX_LEG_CHG".equals(table_name) || "BATCH_TRAN_DTL".equals(table_name)){
				status = 6 ;
			}else if(status!=1 && status != 0){
				status = 1 ;
			}else{
				if(list_table.contains(table_name)){
					status = 0 ;
				}else{
					status = 0 ;
				}
			}
			psmt.setInt(2, status) ;
			if("信息表".equals(aug_info.getTable_type()) ){
				psmt.setString(3, aug_info.getTable_name()+"_MID") ;
				psmt.setString(4, aug_info.getTable_name()+"_TEMP");
			}else{
				if("1".equals(aug_info.getIs_partition())){
					psmt.setString(3, aug_info.getTable_name()+"_MID") ;
				}else{
					psmt.setString(3, "") ;
				}
				psmt.setString(4, "") ;
			}
//			psmt.setTimestamp(4, new Timestamp(System.currentTimeMillis())) ;
			psmt.setTimestamp(5, new Timestamp(System.currentTimeMillis())) ;
			psmt.executeUpdate() ;
		}
		if (psmt != null) {
			psmt.close();
		}
		if (conn != null) {
			conn.close();
		}
	}
	
	
	public void update_augment( String file_path , List<String> list) throws Exception {
		
		AugmentInfoDao aug_info_dao = new AugmentInfoDao() ;
//		Map<String, String> condition_map = new HashMap<String, String>() ;
		Map<String, AugmentInfo> aug_info_map = aug_info_dao.getAugInfoMap(list) ;
		
		Connection conn = DBAccess.getConnection_ds_mysql();
		PreparedStatement psmt = null;
//		psmt = conn.prepareStatement("update tb_table_augment set Addfile_hdfs_script=?,Import_script=?,partition_fun=?,lst_modify_date=? where table_id=?");
		psmt = conn.prepareStatement("update tb_table_augment set addfile_hdfs_script=?,import_script=?,partition_fun=?,create_mid_sql=?,create_temp_sql=?,temp_to_mid=?,lst_modify_date=? where table_id=?");
//		
		Map<String, String> partition_fun_map = new FileListUtil().partition_table_funct(file_path) ;
		
//		HiveConnection hive_conn = new HiveConnection() ;
		//在处理的过程种执行创建中间表的语句
		for(Entry<String, AugmentInfo> entry : aug_info_map.entrySet()){
			String load_sql = "" ;
			String merge_sql = "" ;
			String table_type = "" ;
			String field = "" ;
			String partition_fun = "" ;
			String mid_sql = "" ;
			String temp_sql = "" ;
			String temp_to_mid = "" ;
			AugmentInfo aug_info = entry.getValue() ;
			table_type = aug_info.getTable_type() ;
			load_sql = GeneratLoad.load_sql(aug_info) ;
			field = aug_info.getPartition_field() ;
			if("信息表".equals(table_type)){
				merge_sql = GeneratMerge.sql_generat(aug_info) ;
				mid_sql = MidTableCreateSQL.create_midtable_sql(aug_info) ;
				temp_sql = MidTableCreateSQL.create_temptable_sql(aug_info) ;
				temp_to_mid = MidTableCreateSQL.temp_to_mid_sql(aug_info) ;
				
//				hive_conn.execute(MidTableCreateSQL.create_midtable_sql(aug_info)) ;
				//创建temp临时表  ： 用于去重
			}else{
				if("1".equals(aug_info.getIs_partition())){
//					hive_conn.execute(MidTableCreateSQL.create_midtable_sql(aug_info)) ;
					mid_sql = MidTableCreateSQL.create_midtable_sql(aug_info) ;
				}else{
					mid_sql = "null" ;
				}
				temp_sql = "null" ;
				temp_to_mid = "null" ;
				merge_sql = "null" ;
			}
			psmt.setString(1, load_sql.toLowerCase()) ;
			psmt.setBlob(2, new SerialBlob(merge_sql.getBytes())) ;
			partition_fun = partition_fun_map.get(aug_info.getTable_name()) ;
			if(partition_fun == null || field == null){
				partition_fun = "" ;
			}else{
				partition_fun = partition_fun.replace("partition_column", field) ;
			}
			psmt.setString(3, partition_fun) ;
			
			psmt.setBlob(4, new SerialBlob(mid_sql.getBytes()) );
			psmt.setBlob(5, new SerialBlob(temp_sql.getBytes()));
			psmt.setBlob(6, new SerialBlob(temp_to_mid.getBytes()));
			
//			psmt.setTimestamp(4, new Timestamp(System.currentTimeMillis())) ;
// 			psmt.setInt(5, Integer.parseInt(aug_info.getTable_id())) ;
			
			psmt.setTimestamp(7, new Timestamp(System.currentTimeMillis())) ;
 			psmt.setInt(8, Integer.parseInt(aug_info.getTable_id())) ;
			
			try {
				psmt.executeUpdate() ;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				System.out.println(merge_sql);
				e.printStackTrace();
			}
		}
//		hive_conn.close() ; 
		if (psmt != null) {
			psmt.close();
		}
		if (conn != null) {
			conn.close();
		}
	}
	
	
	public static void main(String[] args) {
		try {
			if(args==null || args.length < 1){
				System.out.println("请输入参数\nargs[0]:分区对应的分区方法\n可输入参数\nargs[1]：需要操作的表,以逗号分割(不进行赋值,默认操作所有表)");
				System.exit(0);
			}
			List<String> list = new ArrayList<String>() ;
			if( args.length > 1 ){
				for(String table_name : args[1].split(",")){
					list.add(table_name) ;
				}
			}
			UpdateMaster um = new UpdateMaster() ;
			um.insert_augment(list) ;
			um.update_augment(args[0] , list) ;
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
