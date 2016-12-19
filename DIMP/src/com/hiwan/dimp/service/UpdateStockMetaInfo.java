package com.hiwan.dimp.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.hiwan.dimp.bean.StockMetaInfo;
import com.hiwan.dimp.dao.StockMetaInfoDAO;
import com.hiwan.dimp.db.DBAccess;

public class UpdateStockMetaInfo {
	public UpdateStockMetaInfo() {
	}
	/**
	 * 修改hbase,hive脚本
	 * @author tx
	 * @throws Exception 
	 */
	// 储存 列名，列类型
	public void updateHiveAndHbaseInfo(StockMetaInfo smi) throws Exception {
		// TODO remove
		System.out.println("连接Oracle......");
		long start=System.currentTimeMillis();
		Connection conn = DBAccess.getConnection_ds_oracle();
		System.out.println("连接Oracle用时:"+(System.currentTimeMillis()-start)+"ms");
		PreparedStatement psmt = null;
		ResultSet rs = null;
		String hiveScript = "";
		// 储存 列名，列类型
		Map<String, String> colInfo = new LinkedHashMap<String, String>();
		String str = allRowListSql(smi.getTable_name());
		
		StockMetaInfoDAO dao = new StockMetaInfoDAO();
		Map<String, String> pm = new HashMap<String, String>();
		pm.put("tablename", smi.getTable_name());
		pm.put("owner", "CPDDS_PDATA");
		
		try{
			// TODO remove
			long blockStart=System.currentTimeMillis();
			Integer blCount = dao.blockCount(pm);
			System.out.println("块用时:"+(System.currentTimeMillis()-blockStart)+"ms,块的大小为:"+blCount);
			
			Integer partitionNum=1;
			
			if(smi.getIs_partition().equals("1")){
				//partitionNum=smi.getPartitionMaplist().size();
				partitionNum=smi.getList().size();
			}
			if(partitionNum==null||partitionNum==0){
				partitionNum=1;
			}
			
			//TODO  桶数量的确定
			Integer splitCount=35000;
			Integer bucketNum=blCount % splitCount==0?blCount/splitCount:blCount/splitCount+1;
			Integer bucketParNum=blCount % (splitCount*partitionNum)==0?blCount/splitCount/partitionNum:blCount/splitCount/partitionNum+1;
				
		
			// TODO remove
			long nameTypeStart=System.currentTimeMillis();
			System.out.println("列名查询语句："+str);
			psmt = conn.prepareStatement(str);
			rs = psmt.executeQuery();
			// 如果表不存在
			//if(!rs.next());
				
			
			System.out.println("列名查询用时:"+(System.currentTimeMillis()-nameTypeStart)+"ms");
			
			
			ResultSetMetaData rsmd = rs.getMetaData();// 列名元数据信息
			int count = rsmd.getColumnCount();// 字段数量
			//String[] colNames = new String[count];

			for (int i = 1; i < count; i++) {
				// colNames[i - 1] = rsmd.getColumnName(i);// 字段名称
				// rsmd.getColumnTypeName(column)
				String type = typeTransform(rsmd.getColumnTypeName(i));
				String name=rsmd.getColumnName(i);
				colInfo.put(name, type);
			} 
			// 列名 类型
			String nameType0 = "";
			String hbaseNameType0 = "";
			for (String key : colInfo.keySet()) {
				nameType0 = nameType0 + (key + " " + colInfo.get(key) + ",");
				hbaseNameType0 = hbaseNameType0 + ("cf:" + key + ",");
			}
			
			String nameType = nameType0.substring(0, nameType0.length() - 1);
			
			//TODO remove
			System.out.println("列名用时:"+(System.currentTimeMillis()-nameTypeStart)+"ms");
		//	String hbaseNameType1 = hbaseNameType0.substring(0, hbaseNameType0.length() - 1);
			// 去掉第一个字段
			//String hbaseNameType =","+hbaseNameType1.substring(hbaseNameType1.indexOf(",") + 1, hbaseNameType1.length());
			
			//String hbaseNameType=","+hbaseNameType1;
			
			//String hbaseScript = "";
			/*if(count==2){//表的字段数目为1时，特殊处理
				hbaseNameType="";
			}*/
			if (smi.getTable_type().equals("信息表")) {
				//hbaseScript = "create " + "'" + smi.getTable_name() + "', 'cf'";
				// hiveScript="create table
				// "+smi.getTable_name()+"("+nameType+") row format delimited
				// fields terminated by ',' stored as textfile;";
				/*hiveScript = "create external table " + smi.getTable_name() + " (rowkey String, " + nameType
						+ ") STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES (\"hbase.columns.mapping\" = \":key"
						+ hbaseNameType + "\") TBLPROPERTIES (\"hbase.table.name\" = \"" + smi.getTable_name() + "\");";*/
				hiveScript="create  table " + smi.getTable_name() + " ("+nameType+")"+"  CLUSTERED BY ("+ smi.getBucket()+ ") INTO "+ bucketNum
						+ " BUCKETS STORED AS ORC TBLPROPERTIES (\"transactional\"=\"true\");";
				if(smi.getIs_partition().equals("1")){
					//bucketNum=blCount % splitCount==0?blCount/splitCount:blCount/splitCount+1;
					hiveScript="create  table " + smi.getTable_name() + " ("+nameType+")"+" partitioned by (p_date string) CLUSTERED BY ("+ smi.getBucket()+ ") INTO "+ bucketParNum
							+ " BUCKETS STORED AS ORC TBLPROPERTIES (\"transactional\"=\"true\");";
				}
			}
			if (smi.getTable_type().equals("明细表")) {
				
				hiveScript = "create table " + smi.getTable_name() + "(" + nameType
						+ ") row format delimited fields terminated by '!' stored as textfile;";
				
				if (smi.getIs_partition().equals("1")) {
					hiveScript = "create table " + smi.getTable_name() + "(" + nameType
							+ ") partitioned by (p_date string) row format delimited fields terminated by '!' stored as textfile;";
				}
			}
			if (smi.getTable_type().equals("汇总表")) {
				hiveScript = "create table " + smi.getTable_name() + "(" + nameType+ ") row format delimited fields terminated by '!' stored as textfile;";
				if (smi.getIs_partition().equals("1")) {
						hiveScript = "create table " + smi.getTable_name() + "(" + nameType+ ") partitioned by (p_date string) row format delimited fields terminated by '!' stored as textfile;";
				}
				
			}
			if (smi.getTable_type().equals("配置表")) {
				hiveScript = "create table " + smi.getTable_name() + "(" + nameType
						+ ") row format delimited fields terminated by '!' stored as textfile;";
			}
			
			smi.setCreate_script_hive(hiveScript);
			//TODO  remove
			long updatestart=System.currentTimeMillis();
			StockMetaInfoDAO.updateStockMetaInfo(smi);
			System.out.println("更新hive字段用时:"+(System.currentTimeMillis()-updatestart)+"ms");
			System.out.println("更新后："+smi);

		} catch (SQLException e) {
			//e.printStackTrace();
			System.out.println("表"+smi.getTable_name()+"在Oracle中不存在,将表状态置为4");
			smi.setStatus("4");
			StockMetaInfoDAO.updateMateStatus(smi);
			return ;
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
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

	public void updateStock(StockMetaInfo smi) {
		
		Connection conn = DBAccess.getConnection_ds_oracle();
		PreparedStatement psmt = null;
		ResultSet rs = null;
		try {//如果不是分区表就插入一条记录
			if(smi.getIs_partition().equals("0")){
				StockMetaInfoDAO.updateStockPartition(smi);
			}else{
				if(smi.getTable_type().equals("明细表")||smi.getTable_type().equals("汇总表")||smi.getTable_type().equals("信息表")){
					// 具体的分区名称 ，分区数据
					psmt = conn.prepareStatement(" select table_name,partition_name from DBA_TAB_PARTITIONS a where table_owner='CPDDS_PDATA' and a.table_name=?");
					psmt.setString(1, smi.getTable_name().toUpperCase());
					rs = psmt.executeQuery();
					//如果表为明细表和汇总表处理
					while (rs.next()) {
						String partion  = rs.getString(2);
						smi.setPartition_value(partion);
						smi.setAdd_partition_script("ALTER TABLE " + smi.getTable_name() + " ADD PARTITION (p_date='"+partion+"');");
						StockMetaInfoDAO.updateStockPartition(smi);
					}
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
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

	public static String allRowListSql(String sql) {
		StringBuilder sb = new StringBuilder("   ");
		sb.append("    select a.*, rownum row_num ");
		sb.append("     from ( ").append(sql).append(" ) a  where 0=1");
		return sb.toString();
	}

	/**
	 * oracle转换成支持hive的数据类型
	 */
	public static String typeTransform(String type) {
		String hiveType = "";
		if (type.equalsIgnoreCase("VARCHAR2") || type.equalsIgnoreCase("CHAR")) {
			hiveType = "string";
		}
		if(type.equalsIgnoreCase("Long")||type.equalsIgnoreCase("CLOB")||type.equalsIgnoreCase("BLOB")||type.equalsIgnoreCase("NCLOB")||type.equalsIgnoreCase("BFILE")){
			hiveType = "string";
		}
		if (type.equalsIgnoreCase("NUMBER")) {
			hiveType = "double";
		}
		// 对oracle日期类型现处理为String
		if (type.equalsIgnoreCase("DATE")||type.equalsIgnoreCase("TIMESTAMP")) {
			hiveType = "string";
		}
		return hiveType;
	}
}
