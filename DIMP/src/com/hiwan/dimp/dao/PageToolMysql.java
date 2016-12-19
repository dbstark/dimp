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
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hiwan.dimp.db.DBAccess;
import com.hiwan.dimp.tool.PageBean;

/**
 * 
 * @author terry
 *
 */
public class PageToolMysql {

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
	private static String allRowListSql(String sql) {
		//sql = select * from (select a.* from (select * from tb_table_increment_date) a) b 
		StringBuilder sb = new StringBuilder("   ");
		sb.append("   ");

		sb.append("   select *  ");
		sb.append("    from (select a.* ");
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
		Connection conn = DBAccess.getConnection_ds_mysql();
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
	private static int getRecords(String sql) throws SQLException {
		int records = 0;
		Connection conn = DBAccess.getConnection_ds_mysql();
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
		
		Connection conn = DBAccess.getConnection_ds_mysql();
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

}
