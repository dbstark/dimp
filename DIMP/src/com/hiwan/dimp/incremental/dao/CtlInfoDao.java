package com.hiwan.dimp.incremental.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.hiwan.dimp.db.DBAccess;
import com.hiwan.dimp.incremental.bean.CtlInfo;

public class CtlInfoDao {

	Connection conn = null;
	Statement psmt = null;

	public CtlInfoDao() {
		try {
			conn = DBAccess.getConnection_ds_mysql();
			psmt = conn.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public CtlInfo get_ctl_info(String job_name_date) {
		CtlInfo ctl_info = null;
		String sql = "select job_name , count(*) from ctl_info where job_name = '" + job_name_date
				+ "' group by job_name";
		ResultSet rs = null;
		try {
			rs = psmt.executeQuery(sql);
			if (rs.next()) {
				ctl_info = new CtlInfo(rs.getString(1), rs.getInt(2));
			} else {
				ctl_info = new CtlInfo(job_name_date, 0); // 如果没有对应的数据文件个数记录,则把job对应的数据文件个数定义为0
			}
			rs.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ctl_info;
	}

	public void close() {
		try {
			if (psmt != null) {
				psmt.close();
			}
			if (conn != null) {
				conn.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
