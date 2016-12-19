package com.hiwan.dimp.incremental.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.hiwan.dimp.db.DBAccess;
import com.hiwan.dimp.incremental.bean.AugmentLog;

public class AugmentLogDao {

	public static void insert_aug_log(AugmentLog aug_log){
		
		Connection conn = DBAccess.getConnection_ds_mysql();
		PreparedStatement psmt = null;
		try {
			psmt = conn.prepareStatement("insert into tb_table_augment_log(augment_id,begin_time,end_time,source_path,source_rownum,status) values(?,?,?,?,?,?)");
			psmt.setInt(1, aug_log.getAugment_id()) ;
			psmt.setTimestamp(2, aug_log.getBegin_time()) ;
			psmt.setTimestamp(3, aug_log.getEnd_time()) ;
			psmt.setString(4, aug_log.getSource_path()) ;
			psmt.setInt(5, aug_log.getSource_rownum()) ;
			psmt.setInt(6, aug_log.getStatus()) ;
			psmt.executeUpdate() ;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
