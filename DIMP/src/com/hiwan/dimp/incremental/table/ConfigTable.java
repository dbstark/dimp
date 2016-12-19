package com.hiwan.dimp.incremental.table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.hiwan.dimp.db.StreamGobbler;
import com.hiwan.dimp.incremental.bean.AugmentInfo;
import com.hiwan.dimp.incremental.bean.IncrementDate;
import com.hiwan.dimp.incremental.bean.SourceFileBean;
import com.hiwan.dimp.incremental.dao.FileLoadingProgress;
import com.hiwan.dimp.incremental.util.HdfsFileSystem;
import com.hiwan.dimp.incremental.util.HiveConnection2;

public class ConfigTable {

	/**
	 * 处理表类型为配置表的情况
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static boolean config_table_proces(AugmentInfo aug_info, String file_path, HiveConnection2 hive_conn)
			throws IOException, InterruptedException {
		/**
		 * 1.清空目标表 2.load文件到指定目录
		 */
		System.out.println("清空表：" + "truncate table " + aug_info.getTable_name());
		boolean where = hive_conn.execute("truncate table " + aug_info.getTable_name());
		hive_conn.close();

		String load_shell = aug_info.getAddfile_hdfs_script().toLowerCase().replaceAll("data_file_path", file_path);
		Process process = Runtime.getRuntime().exec(load_shell);
		StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "ERROR", " ");
		errorGobbler.start();
		StreamGobbler outGobbler = new StreamGobbler(process.getInputStream(), "STDOUT", " ");
		outGobbler.start();
		process.waitFor();

		return where;
	}

	public static boolean config_table_proces(AugmentInfo aug_info, List<SourceFileBean> source_bean_list,
			HiveConnection2 hive_conn) throws IOException, InterruptedException {
		/**
		 * 1.清空目标表 2.load文件到指定目录
		 */
		System.out.println("清空表：" + "truncate table " + aug_info.getTable_name());
		boolean where = hive_conn.execute("truncate table " + aug_info.getTable_name());
		// hive_conn.close() ;
		// String file_path ;
		String file_code_change_path;
		for (SourceFileBean source_bean : source_bean_list) {
			// file_path = source_bean.getSource_path() ;
			System.out.println("存放文件到文件系统:" + new Date());
			file_code_change_path = source_bean.getMid_source_path();
			String load_shell = aug_info.getAddfile_hdfs_script().toLowerCase().replaceAll("data_file_path",
					file_code_change_path);
			Process process = Runtime.getRuntime().exec(load_shell);
			StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "ERROR", " ");
			errorGobbler.start();
			StreamGobbler outGobbler = new StreamGobbler(process.getInputStream(), "STDOUT", " ");
			outGobbler.start();
			process.waitFor();
			System.out.println("存放文件到文件系统end:" + new Date());
		}
		return where;
	}

	public static String config_table_proces(AugmentInfo aug_info, List<SourceFileBean> source_bean_list,
			HiveConnection2 hive_conn, HdfsFileSystem hfs, IncrementDate incr_date) throws Exception {
		/**
		 * 1.清空目标表 2.load文件到指定目录
		 */
		/**
		 * 对source_bean_list进行操作 ， 获取最新的数据
		 */
		String old_time = incr_date.getFile_date();
		String file_date = source_bean_list.get(0).getFile_date();

		if (file_date.compareTo(old_time) < 0) {
			// source_bean_list.clear() ;
			return "";
		} else if (file_date.compareTo(old_time) > 0) {
			System.out.println("清空表：" + "truncate table " + aug_info.getTable_name());
			try {
				hive_conn.execute3("truncate table " + aug_info.getTable_name());
				FileLoadingProgress.updateJobProgress(source_bean_list, 30);
			} catch (Exception e) {
				FileLoadingProgress.updateJobFinalStatus(source_bean_list, "异常", e, "F", "清空目标表：" +
						aug_info.getTable_name() + "\n\t" + e.getMessage(), 4);
				throw e;
			}
		}

		String file_code_change_path;
		/*
		 * if(incr_date instanceof IncrementDate &&
		 * (file_date.compareTo(incr_date.getFile_date()) > 0)){
		 * System.out.println("清空表：" + "truncate table " +
		 * aug_info.getTable_name()); hive_conn.execute3("truncate table " +
		 * aug_info.getTable_name()) ; }
		 */
		for (SourceFileBean source_bean : source_bean_list) {
			String fileName = source_bean.getSource_path();
			/********************
			 * new add [BEGIN]********************
			 * 
			 * @author CHENGKAI.SHENG
			 * @since 2016-12-16
			 * 
			 */
			/******************** new add [END] **********************/

			file_code_change_path = source_bean.getMid_source_path();
			try {
				hfs.put_file_to_hdfs2(file_code_change_path, hive_conn.table_hdfs_path(aug_info.getTable_name()));
				/********************new add [BEGIN]********************
				 * @author CHENGKAI.SHENG
				 * @since 2016-12-16
				 */
				FileLoadingProgress.updateFileProgress(fileName, 90);
			} catch (Exception e) {
				FileLoadingProgress.updateJobFinalStatus(source_bean_list, "异常", e, "F", "本地文件  >>  目标表", 4);
				throw e;
			}
			/******************** new add [END] **********************/

			// hfs.put_file_to_hdfs(file_code_change_path,
			// aug_info.getAddfile_hdfs_script() , aug_info);
		}

		FileLoadingProgress.updateJobFinalStatus(source_bean_list, "成功", null, "F", "END", 2);
		return file_date;
	}
}