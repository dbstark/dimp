/**
 * 
 */
package com.hiwan.dimp.incremental.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import javax.xml.transform.Source;

import org.iq80.snappy.HadoopSnappyCodec;

import com.hiwan.dimp.db.DBAccess;
import com.hiwan.dimp.incremental.bean.SourceFileBean;
import com.sun.org.apache.bcel.internal.classfile.SourceFile;

/**
 * 单文件加载进度跟踪服务中，向表tb_file_loading_progress【插入(insert)、更新(update)】记录
 * @author CHENGKAI.SHENG 
 * @since 2016-12-15
 * 修改日志： 
 *  # 2016-12-15, 新建类 FileLoadingProgress
 * 
 */
public class FileLoadingProgress {

	private static Connection conn = null;
	private static Statement stmt = null;

	//初始化conn和stmt
	public static void connect2Mysql() {
		try {
			conn = DBAccess.getConnection_ds_mysql();
			stmt = conn.createStatement();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 關閉conn和stmt 
	 */
	public static void closeConnection() {
		try {
			if(stmt != null) {
				stmt.close();
			}
  		
			if(conn != null) {
				conn.close();
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 向表tb_file_loading_progress插入一条记录 
	 * @param pr 为表的一条记录，除开自增id字段
	 */
	public static void insertTableFileLoadingProgressValues(ProgressRecord pr) {
		String sql = "insert into tb_file_loading_progress values (null,";

		sql = sql + "'" + pr.jobName + "','" + pr.fileName + "', '" + pr.loadType + "', " + pr.progressPercent + ", "
				+ pr.isSucessful + ", '" + pr.loadInfo + "', '" + pr.startTime + "', '" + pr.endTime + "')";
		System.out.println(sql);
		try {
			stmt.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
    
	/**
	 * 更新tb_file_loading_progress表的progress_percent字段
	 * @param fileName 需要跟踪的文件名
	 * @param progress 文件加载进度
	 */
	public static void updateFileProgress(String fileName, double progress) {

		String sql = "update tb_file_loading_progress set progress_percent = " + progress + " where file_name = '"
				+ fileName + "'";
		System.out.println(sql);

		try {
			stmt.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 更新tb_file_loading_progress表的end_time字段
	 * @param fileName 需要跟踪的文件名
	 * @param endTime  该文件加载结束时间
	 */
	public static void updateEndTime(String fileName, String endTime) {
		String sql = "update tb_file_loading_progress set end_time = '" + endTime + "' where file_name = '" + fileName
				+ "'";
		System.out.println(sql);

		try {
			stmt.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 更新tb_file_loading_progress表的load_info字段
	 * @param fileName 需要跟踪的文件名
	 * @param loadInfo 文件加载是否成功及出错信息
	 */
	public static void updateLoadInfo(String fileName, String loadInfo) {
		String sql = "update tb_file_loading_progress set load_info = '" + loadInfo + "' where file_name = '" + fileName
				+ "'";
		System.out.println(sql);

		try {
			stmt.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 更新tb_file_loading_progress表is_sucessful的字段
	 * @param fileName 需要跟踪的文件名
	 * @param isSucessful 文件加载是否成功标志，1处理中、2成功、3意外中断、4出现异常
	 */
	public static void updateIsSuccessful(String fileName, int isSucessful) {

		String sql = "update tb_file_loading_progress set is_sucessful = " + isSucessful + " where file_name = '"
				+ fileName + "'";
		System.out.println(sql);

		try {
			stmt.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * 转码出错后更新表字段end_time，is_sucessful和load_info
	 * @param fileName
	 * @param endTime
	 * @param isSucessful
	 * @param loadInfo
	 */
	public static void updateFinalStatus(String fileName, String endTime, int isSucessful, String loadInfo) {
		String sql = "update tb_file_loading_progress set end_time = '" + endTime + 
				"', is_sucessful = " + isSucessful + 
				", load_info = '" + loadInfo + "' where file_name = '" +
				fileName + "'";
		
		System.out.println(sql);
		try {
			stmt.execute(sql);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static String formatMessage(String fileName, String messageType, Exception e, String tableType, String stage) {
		String exceptionMessage = "【文  件  名】：" + fileName + "\n" +
								  "【消        息】：" + messageType + "\n\t" + e.getMessage() + "\n" + 
				  				  "【表  类  型】：" + tableType + "\n" + 
				  				  "【错误阶段】：" + stage;
		return exceptionMessage;		
	}
	
	
	public static void updateJobFinalStatus(List<SourceFileBean> fileList, String messageType, Exception e, 
							String tableType, String stage, int status) {
		for (SourceFileBean file : fileList) {
			String fileName = file.getSource_path();
			String loadInfo = formatMessage(fileName, messageType, e, tableType, stage);
			updateFinalStatus(fileName, new Date().toString(), status, loadInfo);
		}
	}
	
	public static void updateJobProgress(List<SourceFileBean> fileList, double progress) {
		for (SourceFileBean file : fileList) {
			String fileName = file.getSource_path();
			updateProgress(fileName, progress);
		}
	}
	/**
	 * 测试【插入、更新】功能
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//ProgressRecord pr = new ProgressRecord("b", "b3.dat", "A", 1, 1, "PROCESSING...", new Date().toString(), null);
		//insertTableFileLoadingProgressValues(pr);

		//FileLoadingProgress.connect2Mysql();
		String fileName = "b3.dat";
		//updateProgress(fileName, 50);
		//updateEndTime(fileName, new Date().toString());
		//updateLoadInfo(fileName, "[STATUS] SUCESSFUL");
		//updateFinalStatus(fileName, 3);
		//updateFinalStatus(fileName, new Date().toString(), 4, "Exception");
		try { 
			System.out.println(1/0);
		} catch (Exception e) {
			System.out.println(formatMessage("b3.dat", "异常", e, "U", "本地文件  >> 临时表"));
		}
		
		HashSet<String> successfile = new HashSet<String>();
		successfile.add("a");
		successfile.add("b");
		successfile.add("c");
		
		for (String file : successfile) {
			System.out.println(file);
		}
	}
}