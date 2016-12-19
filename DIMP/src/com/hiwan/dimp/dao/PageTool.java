package com.hiwan.dimp.dao;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.hiwan.dimp.bean.StockMetaInfo;
import com.hiwan.dimp.db.DBAccess;
import com.hiwan.dimp.tool.PageBean;

/**
 * 
 * @author terry
 *
 */
public class PageTool {
	
	/** TODO  需要更改的东西
	 * user_segments
	 * user_tables
	 * USER_PART_TABLES
	 * USER_PART_KEY_COLUMNS
	 * USER_TAB_PARTITIONS
	 * user_tab_cols
	 * 
	 */
	

	/**
	 * 返回数据总条数
	 * @param sql
	 * @return String
	 * 2015-3-2
	 * totalRecordsSql 
	 * PageTool 
	 *
	 */
	private static String totalRecordsSql(String sql) {
		return "select count(*) records  from (" + sql + ")";
	}

	/**
	 * 返回分页后的数据
	 * @param sql
	 * @param start
	 * @param end
	 * @return String
	 * 2015-3-2
	 * pageRowListSql 
	 * PageTool 
	 *
	 */
	private static String pageRowListSql(String sql, int start, int end) {
		StringBuilder sb = new StringBuilder("   ");
		sb.append("   ");

		sb.append("   select *  ");
		sb.append("    from (select a.*, rownum row_num ");
		sb.append("     from ( ").append(sql).append(" ) a) b  ");
		sb.append("    where b.row_num >").append(start);
		sb.append("   and b.row_num <").append(end);
		return sb.toString();
	}
	
	
	/**
	 * 返回所有数据sql
	 * @param sql
	 * @return String
	 * 2015-3-2
	 * allRowListSql 
	 * PageTool 
	 *
	 */
	public  static String allRowListSql(String sql) {
		StringBuilder sb = new StringBuilder("   ");
		sb.append("   ");

		sb.append("   select *  ");
		sb.append("    from (select a.*, rownum row_num ");
		sb.append("     from ( ").append(sql).append(" ) a) b  ");
		 
		return sb.toString();
	}

	/**
	 * 返回当前数据
	 * 
	 * @param sql
	 * @param start
	 * @param end
	 * @return
	 * @throws SQLException
	 *             List<Map<String,Object>> 2014-12-18 getPageRows PageTool
	 * 
	 */
	private static  List<Map<String, Object>> getPageRows(String sql, int start, int end) throws SQLException {
		PageBean pb = new PageBean();
		Connection conn = DBAccess.getConnection_ds_oracle();
		List<Map<String, Object>> datas = null;
		PreparedStatement psmt = null;
		ResultSet rs = null;
		try {
			psmt = conn.prepareStatement(pageRowListSql(sql, start, end));
			rs = psmt.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();// 列名 元数据信息
			int count = rsmd.getColumnCount();// 字段数量
			String[] colNames = new String[count];

			for (int i = 1; i <= count; i++) {
				colNames[i - 1] = rsmd.getColumnName(i);// 字段名称
			}
			datas = new ArrayList<Map<String, Object>>();
			while (rs.next()) {
				Map<String, Object> item = new HashMap<String, Object>();
				for (int i = 1; i <= count; i++) {
					//item.put(colNames[i - 1].toLowerCase(), rs.getObject(colNames[i - 1]));
					// ==============根据类型封装start===================					 
					switch (rsmd.getColumnType(i)) {
					case Types.VARCHAR:
						item.put(colNames[i - 1].toLowerCase(), rs.getString(colNames[i - 1]));
						break;
					case Types.CHAR:
						item.put(colNames[i - 1].toLowerCase(), rs.getString(colNames[i - 1]));
						break;					 
					case Types.INTEGER:
						item.put(colNames[i - 1].toLowerCase(), rs.getInt(colNames[i - 1]));
						break;
					case Types.TIMESTAMP:
						item.put(colNames[i - 1].toLowerCase(), rs.getTimestamp(colNames[i - 1]));
						break;
					case Types.DATE:
						item.put(colNames[i - 1].toLowerCase(), rs.getDate(colNames[i - 1]));
						break;
					case Types.DOUBLE:
						item.put(colNames[i - 1].toLowerCase(), rs.getDouble(colNames[i - 1]));
						break;
					case Types.FLOAT:
						item.put(colNames[i - 1].toLowerCase(), rs.getFloat(colNames[i - 1]));
						break;
					case Types.CLOB:
						item.put(colNames[i - 1].toLowerCase(), clob2Str(rs.getClob(colNames[i - 1])));
						break;
					case Types.BLOB:
						item.put(colNames[i - 1].toLowerCase(), rs.getBlob(colNames[i - 1]));
						break;
					default:
						item.put(colNames[i - 1].toLowerCase(), rs.getString(colNames[i - 1]));
						break;
					}

					// ==============根据类型封装end=====================
				}
				datas.add(item);
			}
			pb.setRows(datas);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (psmt != null) {
				psmt.close();
			}
			if (conn != null) {
				conn.close();
			}
		}

		return datas;
	}

	/**
	 * 获取总的数据条数
	 * 
	 * @param sql
	 * @return
	 * @throws SQLException
	 *             int 2014-12-18 getRecords PageTool
	 * 
	 */
	public static Integer getRecords(String sql) throws SQLException {
		Integer records = 0;
		Connection conn = DBAccess.getConnection_ds_oracle();
		PreparedStatement psmt = null;
		ResultSet rs = null;
		try {
			psmt = conn.prepareStatement(totalRecordsSql(sql));
			rs = psmt.executeQuery();
			if (rs.next()) {
				records = rs.getInt(1);
			}
		} catch (SQLException e) {

			e.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (psmt != null) {
				psmt.close();
			}
			if (conn != null) {
				conn.close();
			}
		}

		return records;
	}
	/**
	 * 检查表是否存在
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public static Boolean isExist(String tableName) throws SQLException {
		Connection conn = DBAccess.getConnection_ds_oracle();
		PreparedStatement psmt = null;
		ResultSet rs = null;
		try {
			psmt = conn.prepareStatement("select * from dba_tables a where a.owner='CPDDS_PDATA' and table_name=?");
			psmt.setString(1, tableName.toUpperCase());
			rs = psmt.executeQuery();
			if (rs.next()) {
				return true;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (psmt != null) {
				psmt.close();
			}
			if (conn != null) {
				conn.close();
			}
		}

		return false;
	}
	/**
	 * 
	 * @param sql
	 * @param currentPage 当前第几页
	 * @param currentRows 当前每页多少条数据
	 * @return
	 * @throws Exception PageBean
	 * 2014-12-18
	 * getPageBean 
	 * PageTool 
	 *
	 */
	public  static  PageBean getPageBean(String sql, int currentPage, int currentRows) throws Exception {
		PageBean pb = new PageBean();
		int start = 0;
		int end = 0;
		start = (currentPage - 1) * currentRows;
		end = currentPage * currentRows + 1;
		//当前页的记录
		pb.setRows(getPageRows(sql, start, end));		 
		// 总记录数
		int totalRecord = getRecords(sql);
		// 计算总页数
		int totalPage = totalRecord % currentRows == 0 ? totalRecord / currentRows : totalRecord / currentRows + 1; 
		pb.setRecords(totalRecord);
		pb.setPage(currentPage);
		pb.setTotal(totalPage);
		return pb;
	}
 
	/**
	 * 返回所有数据
	 * @param sql
	 * @return
	 * @throws SQLException List<Map<String,Object>>
	 * 2015-3-2
	 * allRowList 
	 * PageTool 
	 *
	 */
	public static List<Map<String, Object>> allRowList(String sql) throws SQLException {

		Connection conn = DBAccess.getConnection_ds_oracle();
		List<Map<String, Object>> datas = null;
		PreparedStatement psmt = null;
		ResultSet rs = null;
		try {
			psmt = conn.prepareStatement(allRowListSql(sql));
			rs = psmt.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();// 列名 元数据信息
			int count = rsmd.getColumnCount();// 字段数量
			String[] colNames = new String[count];

			for (int i = 1; i <= count; i++) {
				colNames[i - 1] = rsmd.getColumnName(i);// 字段名称
			}
			
			datas = new ArrayList<Map<String, Object>>();
			while (rs.next()) {
				Map<String, Object> item = new HashMap<String, Object>();
				for (int i = 1; i <= count; i++) {
					//item.put(colNames[i - 1].toLowerCase(), rs.getObject(colNames[i - 1]));
					// ==============根据类型封装start===================					 
					switch (rsmd.getColumnType(i)) {
					case Types.VARCHAR:
						item.put(colNames[i - 1].toLowerCase(), rs.getString(colNames[i - 1]));
						break;
					case Types.CHAR:
						item.put(colNames[i - 1].toLowerCase(), rs.getString(colNames[i - 1]));
						break;					 
					case Types.INTEGER:
						item.put(colNames[i - 1].toLowerCase(), rs.getInt(colNames[i - 1]));
						break;
					case Types.TIMESTAMP:
						item.put(colNames[i - 1].toLowerCase(), rs.getTimestamp(colNames[i - 1]));
						break;
					case Types.DATE:
						item.put(colNames[i - 1].toLowerCase(), rs.getDate(colNames[i - 1]));
						break;
					case Types.DOUBLE:
						item.put(colNames[i - 1].toLowerCase(), rs.getDouble(colNames[i - 1]));
						break;
					case Types.FLOAT:
						item.put(colNames[i - 1].toLowerCase(), rs.getFloat(colNames[i - 1]));
						break;
					case Types.CLOB:
						item.put(colNames[i - 1].toLowerCase(), clob2Str(rs.getClob(colNames[i - 1])));
						break;
					case Types.BLOB:
						item.put(colNames[i - 1].toLowerCase(), rs.getBlob(colNames[i - 1]));
						break;
					default:
						item.put(colNames[i - 1].toLowerCase(), rs.getString(colNames[i - 1]));
						break;
					}

					// ==============根据类型封装end=====================
				}
				datas.add(item);
			}

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (psmt != null) {
				psmt.close();
			}
			if (conn != null) {
				conn.close();
			}
		}

		return datas;
	}
	
	 

	@SuppressWarnings("unused")
	private static String clobToString(Clob clob) {
		if (clob == null) {
			return null;
		}
		try {
			Reader inStreamDoc = clob.getCharacterStream();

			char[] tempDoc = new char[(int) clob.length()];
			inStreamDoc.read(tempDoc);
			inStreamDoc.close();
			return new String(tempDoc);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException es) {
			es.printStackTrace();
		}

		return null;
	}

	private static String clob2Str(Clob clob) {
		String content = "";
		try {
			Reader is = clob.getCharacterStream();
			BufferedReader buff = new BufferedReader(is);// 得到流
			String line = buff.readLine();
			StringBuffer sb = new StringBuffer();
			while (line != null) {// 执行循环将字符串全部取出付值给StringBuffer由StringBuffer转成STRING
				sb.append(line);
				line = buff.readLine();
			}
			content = sb.toString();
		} catch (Exception e) {

			e.printStackTrace();
		}
		return content;
		
	} 
	
	/**
	 * 得到所有的表
	 * @throws SQLException 
	 */
	public static List<String> allTables(){
		List<String> list=new ArrayList<String>();
		Connection conn = DBAccess.getConnection_ds_oracle();
		PreparedStatement psmt = null;
		ResultSet rs = null;
		try {
			// TODO  hadoop用户
			psmt = conn.prepareStatement("select TABLE_NAME from dba_tables a where a.owner='CPDDS_PDATA'");
			rs = psmt.executeQuery();
			while(rs.next()){
				String tableName=rs.getString("TABLE_NAME");
				list.add(tableName);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally{
			if(rs!=null){
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(psmt!=null){
				try {
					psmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(conn!=null){
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return list;
	}
	/**
	 * 
	 *  @Describe:得到元数据信息
	 *	@author:
	 *  @time  :2015年7月7日 下午3:48:36
	 */
	public static StockMetaInfo getMetafo(String tableName) throws SQLException {
		StockMetaInfo smi=new StockMetaInfo();
		Connection conn = DBAccess.getConnection_ds_oracle();
		PreparedStatement psmt = null;
		ResultSet rs = null;
		//储存 列名，列类型
		Map<String,String> colInfo=new LinkedHashMap<String, String>();
		int rowCount = 0;
		String size;
		
		try {
			String str=allRowListSql(tableName);
			psmt = conn.prepareStatement(str);
			rs = psmt.executeQuery();
			
	        while(rs.next()){
	        	rowCount = rowCount + 1;
	        }
			ResultSetMetaData rsmd = rs.getMetaData();// 列名元数据信息
			int count = rsmd.getColumnCount();// 字段数量
			
			//String[] colNames = new String[count];

			for (int i = 1; i <= count; i++) {
				//colNames[i - 1] = rsmd.getColumnName(i);// 字段名称
				//rsmd.getColumnTypeName(column)
				String type=typeTransform(rsmd.getColumnTypeName(i));
				colInfo.put(rsmd.getColumnName(i),type);
			}
			
			
			rs.close();
			psmt.close();
			
			//select sum(bytes)/1024/1024 M from user_segments where segment_name='ACCINF';
			// TODO  hadoop用户
			psmt=conn.prepareStatement("select floor(sum(bytes)/1024/1024) M from DBA_segments a where 1=1 and a.owner='CPDDS_PDATA' and a.segment_name=?");
			psmt.setString(1, tableName);
			rs=psmt.executeQuery();
			rs.next();
			size=rs.getString("M");
			
			smi.setRow_num(rowCount+"");
			smi.setField_num(count+"");
			smi.setTable_name(tableName);
			smi.setStored_size(size);
			
			
			smi.setBusiness_type("");
			smi.setDeveloper("");
			smi.setAdd_partition_script("");
			smi.setLst_modify_date("");

			rs.close();
			psmt.close();
			//得到表是否分区 ，以及分区字段
			// TODO  hadoop用户
			psmt=conn.prepareStatement("select a.table_name, b.column_name, a.partition_count, a.partitioning_type"
					+ " from DBA_PART_TABLES a, DBA_PART_KEY_COLUMNS b "
					+ " where 1 = 1 and a.owner='CPDDS_PDATA' and b.owner='CPDDS_PDATA' and a.table_name = b.name and a.table_name=?");
			psmt.setString(1, tableName.toUpperCase());
			rs=psmt.executeQuery();
			if(rs.next()){
				smi.setIs_partition("1");
				smi.setPartition_field(rs.getString(2));
			}
			smi.setIs_partition("0");
			smi.setPartition_field("");
			
			//ExcelTool el=new ExcelTool("D:\\Li\\Desktop\\表格-李仕波.xls");
			//StockMetaInfo s= el.queryTableInfo(tableName);
			
			//smi.setTable_type(s.getTable_type());
			//smi.setTarget_table(s.getTarget_table());
			//smi.setBusiness_type(s.getBusiness_type());
			smi.setTarget_type("");
			smi.setStatus("");
			
			//列名 类型
			String nameType0="";
			for (String key : colInfo.keySet()) {
				   nameType0=nameType0+(key+" "+colInfo.get(key)+",");
			}
			String nameType=nameType0.substring(0, nameType0.length()-1);
			
			String hiveScript="";
			String hbaseScript="";
			
			if(smi.getTable_type().equals("信息表")){//创建hbase，生成hive外表，映射hbase
				hbaseScript="create "+ "'"+ tableName+ "', 'cf'";
				hiveScript="create EXTERNAL table "+tableName+"("+nameType+") "
						+ "STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'  ";
						//+ "WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf0:sip,cf1:count") TBLPROPERTIES ("hbase.table.name" = "tableName");";
			}
			if(smi.getTable_type().equals("明细表")){
				hiveScript="";
				
			}
			if(smi.getTable_type().equals("配置表")){
				hiveScript="create table "+tableName+"("+nameType+") row format delimited fields terminated by ',' stored as textfile;";
			}
			if(smi.getTable_type().equals("汇总表")){
				hiveScript="";
			}
			
			/*if(smi.getIs_partition().equals("1")){
				hiveScript="create table "+tableName+"("+nameType+") partitioned by ("+smi.getPartition_field()+" String);";
			}*/
			
			//hiveScript="create table "+tableName+"("+nameType+") row format delimited fields terminated by ',' stored as textfile;";
			smi.setCreate_script_hbase(hbaseScript);
			smi.setCreate_script_hive(hiveScript);
			
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
		return smi;
	}
	/**
	 * 向元数据表中插入数据
	 * @throws SQLException 
	 *  @time  :2015年7月7日 下午4:21:15
	 */
	public static StockMetaInfo insertMetaInfo(StockMetaInfo smi){
		Connection conn = DBAccess.getConnection_ds_mysql();
		PreparedStatement psmt = null;
		ResultSet rs=null;
		try {
			psmt = conn.prepareStatement("insert into tb_table_metadata "
					+ "(table_name,Field_num,Row_num,Stored_size,Is_partition,Partition_field,Table_type,Business_type,Developer,Target_table,Target_type,Create_script_hbase,Create_script_hive,Status,LST_MODIFY_DATE,Clear_DATE ) "
					+ "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",Statement.RETURN_GENERATED_KEYS);
			
			psmt.setString(1, smi.getTable_name());
			psmt.setString(2, smi.getField_num());
			psmt.setString(3, smi.getRow_num());
			psmt.setInt(4,  Integer.parseInt(smi.getStored_size()));
			psmt.setInt(5, Integer.parseInt(smi.getIs_partition()));
			psmt.setString(6, smi.getPartition_field());
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
			
			
			rs = psmt.getGeneratedKeys(); //获取返回主键结果   
			int autoIncKey=0;
			if (rs.next()) {
				autoIncKey=rs.getInt(1);
			}
			smi.setTable_id(autoIncKey+"");
			
			
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
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
		return smi;
	}
	public static StockMetaInfo insertStockInfo(StockMetaInfo smi){
		Connection conn = DBAccess.getConnection_ds_mysql();
		PreparedStatement psmt = null;
		ResultSet rs=null;
		try {
			if(smi.getIs_partition().equals("1")){
				// TODO hadoop用户
				psmt = conn.prepareStatement("select table_name,partition_name from DBA_TAB_PARTITIONS a where a.table_owner='CPDDS_PDATA' and a.table_name=?");
				psmt.setString(1, smi.getTable_name().toUpperCase());
				rs=psmt.executeQuery();
			}
			
			
			psmt = conn.prepareStatement("insert into Tb_table_stock (Table_id,Add_Partition_script,Import_script,LST_MODIFY_DATE,Status) values(?,?,?,?,?)");
			
			psmt.setString(1, smi.getTable_id());
			psmt.setString(2, " ");
			psmt.setString(3, " ");
			psmt.setTimestamp(4, new java.sql.Timestamp(System.currentTimeMillis()));
			psmt.setInt(5, 0);
			
			psmt.executeUpdate();
			
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
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
		return smi;
	}
	
	/**
	 * oracle转换成支持hive的数据类型
	 */
	public static String typeTransform(String type){
		String hiveType="";
		if(type.equalsIgnoreCase("VARCHAR2")||type.equals("CHAR")||type.equalsIgnoreCase("Integer")){
			hiveType="string";
		}
		if(type.equalsIgnoreCase("NUMBER")){
			hiveType="double";
		}
		//对oracle日期类型现处理为String
		if(type.equalsIgnoreCase("DATE")||type.equalsIgnoreCase("TIMESTAMP")){
			hiveType="string";
		}
		return hiveType;
	}
	

}
