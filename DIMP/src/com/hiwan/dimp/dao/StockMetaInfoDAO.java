package com.hiwan.dimp.dao;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.beanutils.BeanUtils;

import com.hiwan.dimp.bean.MataStockLog;
import com.hiwan.dimp.bean.StockMetaInfo;
import com.hiwan.dimp.db.DBAccess;

public class StockMetaInfoDAO {	
	 	
	/**
	 * 获取matedata数据
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes") 
	public List<StockMetaInfo> getMetaInfoList(Map map) throws Exception {
		StringBuilder sql = new StringBuilder("  ");
		sql.append("  SELECT                                 ");
		sql.append("    md.id md_id,                         ");
		sql.append("    table_name,                          ");
		sql.append("    field_num,                           ");
		sql.append("    row_num,                             ");
		sql.append("    stored_size,                         ");
		sql.append("    is_partition,                        ");
		sql.append("    partition_field,                     ");
		sql.append("    table_type,                          ");
		sql.append("    business_type,                       ");
		sql.append("    developer,                           ");
		sql.append("    target_table,                        ");
		sql.append("    target_type,                         ");
		sql.append("    create_script_hbase,                 ");
		sql.append("    create_script_hive,                  ");
		sql.append("    md.status,                           ");
		sql.append("    md.lst_modify_date,                  ");
		sql.append("    clear_date  ,                         ");
		sql.append("    primary_key ,                         ");
		sql.append("    bucket                                ");
		sql.append("  FROM                                   ");
		sql.append("    tb_table_metadata md                 ");
		sql.append("  WHERE 1=1                               ");
		if(map != null&&map.get("beginRow")!=null){
			sql.append(" AND  md.id  > '").append(map.get("beginRow")).append("' ");
		}else if(map != null&&map.get("tableName")!=null){
			sql.append("   AND   table_name='").append(map.get("tableName")).append("' ");
		}else{
			if(map != null&&map.get("status")!=null){
				sql.append("   AND   Status='").append(map.get("status")).append("' ");
			}
			if(map != null&&map.get("is_partition")!=null){
				sql.append("   AND   is_partition='").append(map.get("is_partition")).append("' ");
			}
			if (map != null) {
				sql.append("   AND(                                ");			
				Map<String, String> typeMap = new HashMap<String, String>();	
				if (map.get("xinxi") != null) {
					typeMap.put("xinxi", " md.Table_type ='"+map.get("xinxi")+"'");
				}
				if (map.get("peizhi") != null) {
					typeMap.put("peizhi"," md.Table_type ='"+map.get("peizhi")+"'");			
				}
				if (map.get("mingxi") != null) {
					typeMap.put("mingxi", " md.Table_type ='"+map.get("mingxi")+"'");
				}
				if (map.get("huizong") != null) {
					typeMap.put("huizong"," md.Table_type ='"+map.get("huizong")+"'");
				}					
				String Strsql="";
				for(Entry<String, String> entry : typeMap.entrySet()){			 
						Strsql += entry.getValue()+" or" ;			 				 	 
				}
				Strsql =Strsql.substring(0,Strsql.trim().length()-1);
				sql.append(Strsql);
				sql.append("    )                                    ");
			}
		}
		 // System.out.println(" getMetaInfoList sql :"+sql);
		return this.getObjectList(PageToolMysql.allRowList(sql.toString()));

	}
	/**
	 *  获取Stock数据
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes") 
	public List<StockMetaInfo> getStockInfoList(Map map) throws Exception{
		StringBuilder sql = new StringBuilder("  ");
		sql.append("    	SELECT                                        ");
		sql.append("    	  st.id AS st_id,                             ");
		sql.append("    	  table_id,                                   ");
		sql.append("    	  st.add_partition_script,                    ");
		sql.append("    	  import_script,                              ");
		sql.append("    	  st.partition_value partition_value,          ");
		sql.append("    	  st.lst_modify_date st_lst_modify_date,      ");
		sql.append("    	  st.status AS st_status                      ");
		sql.append("    	FROM                                          ");
		sql.append("    	  tb_table_stock st                           ");
		sql.append("    	WHERE st.table_id='").append(map.get("tableid")).append("' and (st.status='0' or st.status is null)");
		
		//System.out.println(" getStockInfoList sql :"+sql);				                             
	    
		return this.getObjectList(PageToolMysql.allRowList(sql.toString()));
		 
	}
	
	/**
	 * 获取源数据信息
	 * @param map
	 * @return
	 * @throws Exception
	 */
	public List<StockMetaInfo> getInfoList(Map<String, String> map)
			throws Exception {
		List<StockMetaInfo> list = new ArrayList<StockMetaInfo>();
		List<StockMetaInfo> mateList = this.getMetaInfoList(map);
		for (StockMetaInfo mate : mateList) {
			Map<String, String> mateMap = new HashMap<String, String>();
			mateMap.put("tableid", mate.getMd_id());
			//通过tableid 获取Stock信息
			mate.setList(this.getStockInfoList(mateMap));
			if(mate.getIs_partition().equals("1")){
				mate.setPartitionMaplist(this.getPartitionsByTable(mate.getTable_name().toUpperCase()));				
			}
			list.add(mate);
		}
		return list;
	}
	
	
	/**
	 * 对象转换
	 * @param list
	 * @return
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 */
	public List<StockMetaInfo> getObjectList(List<Map<String, Object>> list) throws IllegalAccessException, InvocationTargetException{
		
		List<StockMetaInfo> objectLlist = new ArrayList<StockMetaInfo>();
		for(Map<String, Object> map : list){			
			StockMetaInfo sm = new StockMetaInfo();			
			BeanUtils.populate(sm,map);			
			objectLlist.add(sm);
		}
		return objectLlist;
	}
 
	
	/**
	 * 向元数据表中插入数据
	 * @throws SQLException 
	 *  @time  :2015年7月7日 下午4:21:15
	 */
	public static void insertMetaInfo(StockMetaInfo smi) {
		Connection conn = DBAccess.getConnection_ds_mysql();
		PreparedStatement psmt = null;
		try {
			psmt = conn.prepareStatement("insert into tb_table_metadata "
					+ "(table_name,Field_num,Row_num,Stored_size,Is_partition,Partition_field,Table_type,Business_type,Developer,Target_table,Target_type,Create_script_hbase,Create_script_hive,Status,LST_MODIFY_DATE,Clear_DATE ) "
					+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
			
			psmt.setString(1, smi.getTable_name());
			psmt.setString(2, smi.getField_num());
			psmt.setString(3, smi.getRow_num());
			psmt.setInt(4,  Integer.parseInt(smi.getStored_size()));
			psmt.setInt(5, 0);
			psmt.setInt(6, 0);
			psmt.setString(7, smi.getTable_type());
			psmt.setString(8, smi.getBusiness_type());
			psmt.setString(9, smi.getDeveloper());
			psmt.setString(10, smi.getTarget_table());
			psmt.setString(11, "Hive");
			psmt.setString(12, smi.getCreate_script_hbase());
			psmt.setString(13, smi.getCreate_script_hive());
			psmt.setInt(14, 0);
			//psmt.setDate(15, new java.sql.Date(System.currentTimeMillis()));
			psmt.setTimestamp(15, new java.sql.Timestamp(System.currentTimeMillis()));
			
			psmt.setInt(16, 0);
			//psmt.execute("set names UTF-8");
			psmt.executeUpdate();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			try {
				if (psmt != null) {
					psmt.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	/**
	 * 向Tb_table_stock_log插入日志文件
	 * @param args
	 * @throws Exception 
	 */
	public static void insertLogInfo(StockMetaInfo smi,MataStockLog msl) {
		Connection conn = DBAccess.getConnection_ds_mysql();
		PreparedStatement psmt = null;
		try {
			psmt = conn.prepareStatement("insert into tb_table_stock_log(Stock_id,Begin_time,end_time,Source_rownum,Target_rownum,Status,partion_id) values(?,?,?,?,?,?,?) ");
			psmt.setInt(1, Integer.parseInt(smi.getMd_id()));//表ID
			psmt.setTimestamp(2, new java.sql.Timestamp(Long.parseLong(msl.getBegin_time())));
			psmt.setTimestamp(3, new java.sql.Timestamp(Long.parseLong(msl.getEnd_time())));
			psmt.setInt(4, msl.getSource_rownum()==null?0:Integer.parseInt(msl.getSource_rownum()));
			psmt.setInt(5, msl.getTarget_rownum()==null?0:Integer.parseInt(msl.getTarget_rownum()));
			psmt.setInt(6, msl.getStatus()==null?0:Integer.parseInt(msl.getStatus()));
			psmt.setInt(7, msl.getPartion_id()==null?-1:Integer.parseInt(msl.getPartion_id()));
			//psmt.execute("set names UTF-8");
			psmt.executeUpdate();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			try {
				if (psmt != null) {
					psmt.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 更新建表脚本
	 * @param smi
	 */
	public static void updateStockMetaInfo(StockMetaInfo smi){
		Connection conn = DBAccess.getConnection_ds_mysql();
		PreparedStatement psmt = null;
		try {
			psmt = conn.prepareStatement("update tb_table_metadata set Create_script_hive=?,LST_MODIFY_DATE=? where table_name=?");
			//psmt.setString(1, smi.getCreate_script_hbase());
			psmt.setString(1, smi.getCreate_script_hive());
			psmt.setTimestamp(2, new java.sql.Timestamp(System.currentTimeMillis()));
			psmt.setString(3, smi.getTable_name().toUpperCase());
			psmt.executeUpdate();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			try {
				if (psmt != null) {
					psmt.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		
	} 
	/**
	 * 插入分区脚本
	 * @param smi
	 */
	public static void updateStockPartition(StockMetaInfo smi){
		Connection conn = DBAccess.getConnection_ds_mysql();
		PreparedStatement psmt = null;
		try {
			psmt = conn.prepareStatement("insert into tb_table_stock(table_id,Add_Partition_script,Import_script,LST_MODIFY_DATE,partition_value) values(?,?,?,?,?)");
			psmt.setString(1, smi.getMd_id());
			psmt.setString(2,smi.getAdd_partition_script()==null?"":smi.getAdd_partition_script());
			psmt.setString(3, smi.getImport_script()==null?"":smi.getImport_script());
			psmt.setTimestamp(4, new java.sql.Timestamp(System.currentTimeMillis()));
			psmt.setString(5, smi.getPartition_value()==null?"":smi.getPartition_value());
			psmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			try {
				if (psmt != null) {
					psmt.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	} 

	/**
	 * 调试后更新状态为正常
	 * @param smi
	 */
	public static void updateMateStatus(StockMetaInfo smi){
		Connection conn = DBAccess.getConnection_ds_mysql();
		PreparedStatement psmt = null;
		try {
			psmt = conn.prepareStatement("update tb_table_metadata set Status=?,LST_MODIFY_DATE=? where table_name=?");
			psmt.setString(1, smi.getStatus());
			psmt.setTimestamp(2 ,new java.sql.Timestamp(System.currentTimeMillis()));
			psmt.setString(3, smi.getTable_name());
			psmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			try {
				if (psmt != null) {
					psmt.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	}
	/**
	 * 更新tb_table_stock status信息，1表示已经执行加载过分区数据
	 * @param smi
	 */
	public static void updateStockStatus(StockMetaInfo smi){
		Connection conn = DBAccess.getConnection_ds_mysql();
		PreparedStatement psmt = null;
		try {
			psmt = conn.prepareStatement("update tb_table_stock set Status=?,LST_MODIFY_DATE=? where table_id=? and partition_value=?");
			psmt.setString(1, smi.getSt_status());
			psmt.setTimestamp(2 ,new java.sql.Timestamp(System.currentTimeMillis()));
			psmt.setString(3, smi.getMd_id());
			psmt.setString(4, smi.getPartition_value());
			psmt.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			try {
				if (psmt != null) {
					psmt.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	}
	/**
	 * //查询表所有分区的状态是不是都成功
	 * @param smi
	 */
	public static Boolean queryTablePartionStatus(StockMetaInfo smi){
		Connection conn = DBAccess.getConnection_ds_mysql();
		PreparedStatement psmt = null;
		ResultSet rs;
		try {
			psmt = conn.prepareStatement("SELECT s.Status FROM tb_table_metadata m,tb_table_stock s WHERE m.id=s.Table_id  AND m.table_name=?");
			psmt.setString(1, smi.getTable_name().toUpperCase());
			rs=psmt.executeQuery();
			while(rs.next()){
				Integer status=rs.getInt(1);
				if(status!=1){
					return false;
				}
			}
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			try {
				if (psmt != null) {
					psmt.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return false;
		
	}
	
	
	/**
	 * 根据表明获取文件块数量
	 * @param map
	 * @return
	 * @throws Exception
	 */
	public Integer blockCount(Map<String, String> map ) throws Exception{
		StringBuilder sql = new StringBuilder();
		sql.append("	select sum(blocks) as num			 ");
		sql.append("	  from (SELECT e.blocks		 ");
		sql.append("	          FROM dba_extents e, dba_objects o, dba_tab_subpartitions tsp		 ");
		sql.append("	        WHERE o.OWNER = '").append(map.get("owner")).append("' ");
		sql.append("	          AND o.object_name = '").append(map.get("tablename")).append("'");
		sql.append("	           AND e.OWNER = '").append(map.get("owner")).append("' ");
		sql.append("	           AND e.segment_name = '").append(map.get("tablename")).append("'");	
		sql.append("	           AND o.owner = e.owner		 ");
		sql.append("	           AND o.object_name = e.segment_name		 ");
		sql.append("	AND (o.subobject_name = e.partition_name OR		 ");
		sql.append("	               (o.subobject_name IS NULL AND e.partition_name IS NULL))		 ");
		sql.append("	          AND o.owner = tsp.table_owner(+)		 ");
		sql.append("	           AND o.object_name = tsp.table_name(+)		 ");
		sql.append("	           AND o.subobject_name = tsp.subpartition_name(+)		 ");
		
		if(map!=null&&map.get("partition_value")!=null){
			sql.append("	           AND case      ");
			sql.append("	                 when o.object_type = 'TABLE SUBPARTITION' then     ");
			sql.append("	                  tsp.partition_name     	");
			sql.append("	                 else     ");
			sql.append("	                  o.subobject_name    	 ");
			sql.append("					end IN('"+map.get("partition_value")+"')");

		}
		
		sql.append("	        ) t");
		//System.out.println(sql.toString());
		Map<String, Object> obj = PageTool.allRowList(sql.toString()).get(0);
		//System.out.println("sql:"+sql);
		//Integer value=PageTool.getRecords(sql.toString());
		//return value==null?0:value;
		return Integer.valueOf(obj.get("num")==null?"0":obj.get("num").toString());
	}
	/**
	 * 根据表明获取分区名称
	 * TODO  注意不同oracle用户权限问题
	 * @param tablename
	 * @return
	 * @throws Exception
	 */
	public List<Map<String, Object>> getPartitionsByTable(String tablename) throws Exception{		 
		StringBuilder sql = new StringBuilder();
		sql.append(" select table_name,partition_name from DBA_TAB_PARTITIONS a where table_owner='CPDDS_PDATA' and a.table_name='").append(tablename.toUpperCase()).append("' ");		
		return PageTool.allRowList(sql.toString());
	}
	
	/**
	 * 根据表明获取
	 * TODO  注意不同oracle用户权限问题
	 * @param tablename
	 * @return
	 * @throws Exception
	 */
	public List<Map<String, Object>> getPartitionInfoByTable(String tablename) throws Exception{		 
		StringBuilder sql = new StringBuilder();
		sql.append("      select a.table_name,                     ");
		sql.append("             b.column_name,                    ");
		sql.append("             c.data_type,                      ");
		sql.append("             a.partition_count,                ");
		sql.append("             a.partitioning_type,              ");
		sql.append("             d.partition_name                  ");
		sql.append("        from DBA_PART_TABLES      a,          ");
		sql.append("             DBA_PART_KEY_COLUMNS b,          ");
		sql.append("             DBA_tab_cols         c,          ");
		sql.append("             DBA_TAB_PARTITIONS   d           ");
		sql.append("       where 1 = 1                             ");
		sql.append("         and a.table_name = b.name   and a.owner='CPDDS_PDATA'          ");
		sql.append("         and a.table_name = c.TABLE_NAME  and b.owner='CPDDS_PDATA'     ");
		sql.append("         and c.COLUMN_NAME = b.column_name  and c.owner='CPDDS_PDATA'   ");
		sql.append("         and a.table_name = d.table_name       ");
		sql.append("         and a.table_name ='").append(tablename.toUpperCase()).append("' ");			
		return PageTool.allRowList(sql.toString());
	}
	/**
	 * 
	 * 增量
	 * @throws Exception
	 */
	public static void insertAugmentInfo(StockMetaInfo smi,MataStockLog msl) {
		Connection conn = DBAccess.getConnection_ds_mysql();
		PreparedStatement psmt = null;
		try {
			psmt = conn.prepareStatement("insert into tb_table_augment(Table_id,Mid_table_name,Addfile_ hdfs_script,Duplicate_removal,Import_script,Status,LST_MODIFY_DATE) values(?,?,?,?,?,?,?) ");
			psmt.setInt(1, Integer.parseInt(smi.getMd_id()));
			psmt.setTimestamp(2, new java.sql.Timestamp(Long.parseLong(msl.getBegin_time())));
			psmt.setTimestamp(3, new java.sql.Timestamp(Long.parseLong(  msl.getEnd_time())));
			psmt.setInt(4, Integer.parseInt(msl.getSource_rownum()));
			psmt.setInt(5, Integer.parseInt(msl.getTarget_rownum()));
			psmt.setInt(6, Integer.parseInt(msl.getStatus()));
			psmt.setString(7, msl.getPartion_id());
			//psmt.execute("set names UTF-8");
			psmt.executeUpdate();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			try {
				if (psmt != null) {
					psmt.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	
	public static  void main(String[] args) throws Exception {
		
		
		 
		//StockMetaInfo smi=new StockMetaInfo();
		StockMetaInfoDAO dao = new StockMetaInfoDAO();
		//smi.setTable_name("TCS_PARTY_AGMT_RELA_H");
		//StockMetaInfoDAO dao = new StockMetaInfoDAO();
		//Boolean a=StockMetaInfoDAO.queryTablePartionStatus(smi);
		Map<String, String> pm = new HashMap<String, String>();
		pm.put("tablename", "BB_CUSTOMER");
		pm.put("owner", "CPDDS_PDATA");
		//pm.put("partition_value", "P1");
		Integer value=dao.blockCount(pm);
		System.out.println(value);
		
		/*Map map = new HashMap();
		map.put("tablename", "CSTM_PRSN_BASE");
		map.put("owner","CPDDS_PDATA");
		Integer blockCount = dao.blockCount(map);*/
		/*Map map = new HashMap();
		map.put("mingxi", "明细表");
		map.put("huizong","汇总表");
		map.put("status", "0");
		List<StockMetaInfo> list = dao.getInfoList(map);
		System.out.println("getInfoList :"+list.size());*/
		
		/*int i=0;
		for(StockMetaInfo sm : list){
			System.out.println("Primary_key:"+sm.getPrimary_key());
			i++;
			System.out.println( sm.getMd_id()+"  :"+i+"  ===== "+sm.getList().size());
			 
			System.out.println("------------");
		}*/ 
		
		
		 
		 
		/*
		List<Map<String, Object>> mapList =  dao.getPartitionsByTable("TCS_PARTY_AGMT_RELA_H");
		for(Map<String, Object> map :mapList){
			System.out.println("map:"+map.get("table_name")+ " partition_name :"+map.get("partition_name"));
		}
		*/
		
	}
}
