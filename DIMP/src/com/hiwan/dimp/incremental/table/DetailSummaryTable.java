package com.hiwan.dimp.incremental.table;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.hiwan.dimp.db.StreamGobbler;
import com.hiwan.dimp.incremental.bean.AugmentInfo;
import com.hiwan.dimp.incremental.bean.SourceFileBean;
import com.hiwan.dimp.incremental.dao.FileLoadingProgress;
import com.hiwan.dimp.incremental.util.HdfsFileSystem;
import com.hiwan.dimp.incremental.util.HiveConnection2;

public class DetailSummaryTable {

	/**
	 * 处理明细表和汇总表
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws Exception
	 */
	public static boolean detail_summary_table_proces(AugmentInfo aug_info, String file_path, HiveConnection2 hive_conn)
			throws Exception {
		/**
		 * 1.判断表是否为分区表 分区表：创建分区,load数据到分区 非分区表：直接load文件到hdfs目录
		 */
		String is_partition = aug_info.getIs_partition();
		String load_shell = aug_info.getAddfile_hdfs_script().toLowerCase().replace("data_file_path", file_path);

		/**
		 * 1.分区表: 1)清空中间表 2)load data to mid_table 2.非分区表":load data to
		 * target_table
		 */
		if ("1".equals(is_partition)) {
			System.out.println("清空表：" + "truncate table " + aug_info.getTable_name());
			hive_conn.execute("truncate table " + aug_info.getTable_name());
		}

		Process process = Runtime.getRuntime().exec(load_shell);
		StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "ERROR", " ");
		errorGobbler.start();
		StreamGobbler outGobbler = new StreamGobbler(process.getInputStream(), "STDOUT", " ");
		outGobbler.start();
		process.waitFor();

		boolean where = true;
		if ("1".equals(is_partition)) {
			/**
			 * 分区表 1.load数据到中间表中 2.检索出中间表中 对应 oracle 分区字段的value list
			 * 3.根据oracle的转换和分区规则列出 oracle_partition - hive_partition 对应关系
			 * 4.在target表中创建分区 5.根据oracle_partition - hive_partition
			 * 对应关系把数据导入目标表
			 */

			String mid_table_name = aug_info.getMid_table_name();
			String table_name = aug_info.getTable_name();
			String partition_field = aug_info.getPartition_field();
			String partition_fun = aug_info.getPartition_fun();
			System.out.println(partition_fun);
			String sql = "select " + partition_fun + " par from " + mid_table_name + " group by par ";
			System.out.println("sql:" + sql);
			ResultSet rs = hive_conn.execute2(sql);
			List<String> par_list = new ArrayList<String>();
			String par_value;
			while (rs.next()) {
				par_value = rs.getString(1);
				System.out.println("partition_value:" + par_value);
				par_list.add(par_value);
			}
			rs.close();
			/**
			 * 创建分区 执行插入语句
			 */
			for (String partition_value : par_list) {
				hive_conn.execute(" alter table " + table_name + " add if not exists partition(p_date='"
						+ partition_value + "')");
				where = hive_conn.execute("insert into table " + table_name + " partition (p_date='" + partition_value
						+ "') select * from " + mid_table_name + " where " + partition_fun + " = '" + partition_value
						+ "' ");
			}
			/*
			 * while(rs.next()){ String partition_value = rs.getString(1) ;
			 * System.out.println("partition_value:"+partition_value); //
			 * System.out.println(" alter table " + table_name +
			 * " add if not exists partition(p_date='" + partition_value +
			 * "')"); // System.out.println("insert into table " + table_name +
			 * " partition (p_date='" + partition_value + "')  select * from " +
			 * mid_table_name + " where " + partition_fun + " = '" +
			 * partition_value + "' "); hive_conn.execute(" alter table " +
			 * table_name + " add if not exists partition(p_date='" +
			 * partition_value + "')") ; hive_conn.execute("insert into table "
			 * + table_name + " partition (p_date='" + partition_value +
			 * "') select * from " + mid_table_name + " where " + partition_fun
			 * + " = '" + partition_value + "' ") ; }
			 */

			hive_conn.close();
		}
		return where;
	}

	public static boolean detail_summary_table_proces(AugmentInfo aug_info, List<SourceFileBean> source_bean_list,
			HiveConnection2 hive_conn) throws Exception {
		/**
		 * 1.判断表是否为分区表 分区表：创建分区,load数据到分区 非分区表：直接load文件到hdfs目录
		 */
		String is_partition = aug_info.getIs_partition();

		/**
		 * 1.分区表: 1)清空中间表 2)load data to mid_table 2.非分区表":load data to
		 * target_table
		 */
		if ("1".equals(is_partition)) {
			System.out.println("清空表：" + "truncate table " + aug_info.getTable_name());
			hive_conn.execute("truncate table " + aug_info.getTable_name());
		}

		for (SourceFileBean source_bean : source_bean_list) {
			String load_shell = aug_info.getAddfile_hdfs_script().toLowerCase().replace("data_file_path",
					source_bean.getMid_source_path());
			Process process = Runtime.getRuntime().exec(load_shell);
			StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "ERROR", " ");
			errorGobbler.start();
			StreamGobbler outGobbler = new StreamGobbler(process.getInputStream(), "STDOUT", " ");
			outGobbler.start();
			process.waitFor();
		}

		boolean where = true;
		if ("1".equals(is_partition)) {
			/**
			 * 分区表 1.load数据到中间表中 2.检索出中间表中 对应 oracle 分区字段的value list
			 * 3.根据oracle的转换和分区规则列出 oracle_partition - hive_partition 对应关系
			 * 4.在target表中创建分区 5.根据oracle_partition - hive_partition
			 * 对应关系把数据导入目标表
			 */

			String mid_table_name = aug_info.getMid_table_name();
			String table_name = aug_info.getTable_name();
			String partition_field = aug_info.getPartition_field();
			String partition_fun = aug_info.getPartition_fun();
			System.out.println(partition_fun);
			String sql = "select " + partition_fun + " par from " + mid_table_name + " group by par ";
			System.out.println("sql:" + sql);
			ResultSet rs = hive_conn.execute2(sql);
			List<String> par_list = new ArrayList<String>();
			String par_value;
			while (rs.next()) {
				par_value = rs.getString(1);
				System.out.println("partition_value:" + par_value);
				par_list.add(par_value);
			}
			/**
			 * 创建分区 执行插入语句
			 */
			for (String partition_value : par_list) {
				hive_conn.execute(" alter table " + table_name + " add if not exists partition(p_date='"
						+ partition_value + "')");
				where = hive_conn.execute("insert into table " + table_name + " partition (p_date='" + partition_value
						+ "') select * from " + mid_table_name + " where " + partition_fun + " = '" + partition_value
						+ "' ");
			}
			/*
			 * while(rs.next()){ String partition_value = rs.getString(1) ;
			 * System.out.println("partition_value:"+partition_value); //
			 * System.out.println(" alter table " + table_name +
			 * " add if not exists partition(p_date='" + partition_value +
			 * "')"); // System.out.println("insert into table " + table_name +
			 * " partition (p_date='" + partition_value + "')  select * from " +
			 * mid_table_name + " where " + partition_fun + " = '" +
			 * partition_value + "' "); hive_conn.execute(" alter table " +
			 * table_name + " add if not exists partition(p_date='" +
			 * partition_value + "')") ; hive_conn.execute("insert into table "
			 * + table_name + " partition (p_date='" + partition_value +
			 * "') select * from " + mid_table_name + " where " + partition_fun
			 * + " = '" + partition_value + "' ") ; }
			 */

			// hive_conn.close() ;
		}
		return where;
	}

	public static String detail_summary_table_proces(AugmentInfo aug_info, List<SourceFileBean> source_bean_list,
			HiveConnection2 hive_conn, HdfsFileSystem hfs) throws Exception {
		/**
		 * 1.判断表是否为分区表 分区表：创建分区,load数据到分区 非分区表：直接load文件到hdfs目录
		 */
		String is_partition = aug_info.getIs_partition();

		/**
		 * 1.分区表: 1)清空中间表 2)load data to mid_table 2.非分区表":load data to
		 * target_table
		 */
		if ("1".equals(is_partition)) {
			System.out.println("清空表：" + "truncate table " + aug_info.getMid_table_name());
			try {
				hive_conn.execute3("truncate table " + aug_info.getMid_table_name());
				//处理正常，未结束
				FileLoadingProgress.updateJobProgress(source_bean_list, 25);
			} catch (Exception e) {
				//处理异常
				FileLoadingProgress.updateJobFinalStatus(source_bean_list, "", e, "A", "清空中间表：" +
							aug_info.getMid_table_name() + "\n\t" + e.getMessage(), 4);
				throw e;
			}
		}

		String file_date = null;
		String[] f_arr = null;
		for (SourceFileBean source_bean : source_bean_list) {
			/********************
			 * new add [BEGIN]********************
			 * 
			 * @author CHENGKAI.SHENG
			 * @since 2016-12-16
			 * 
			 */
			String fileName = source_bean.getSource_path();
			/******************** new add [END] *********************/

			f_arr = source_bean.getSource_path().split("_");
			if (file_date == null || file_date.compareTo(f_arr[f_arr.length - 3]) < 0) {
				file_date = f_arr[f_arr.length - 3];
			}
			String load_shell = aug_info.getAddfile_hdfs_script().toLowerCase();
			String file_code_change_path = source_bean.getMid_source_path();
			String hdfs_path = "";
			if ("1".equals(is_partition)) {
				hdfs_path = hive_conn.table_hdfs_path(aug_info.getMid_table_name());
			} else {
				hdfs_path = hive_conn.table_hdfs_path(aug_info.getTable_name());
			}
			// hfs.put_file_to_hdfs(file_code_change_path, load_shell,aug_info)
			// // ;
			try {
				hfs.put_file_to_hdfs2(file_code_change_path, hdfs_path);
				/**********************
				 * new add [BEGIN]********************
				 * 
				 * @author CHENGKAI.SHENG
				 * @since 2016-12-16
				 * 
				 */
				if ("1".equals(is_partition)) {
					//加载正常，未结束
					FileLoadingProgress.updateFileProgress(fileName, 45);
				} else {
					//加载正常，结束
					FileLoadingProgress.updateFileProgress(fileName, 90);
				}
			} catch (Exception e) {
				FileLoadingProgress.updateJobFinalStatus(source_bean_list, "异常", e, "A", "本地文件  >>  中间表\n\t" +
											e.getMessage(), 4);
				throw e;
			}
			/******************** new add [END] *********************/
		}

		if ("1".equals(is_partition)) {
			/**
			 * 分区表 1.load数据到中间表中 2.检索出中间表中 对应 oracle 分区字段的value list
			 * 3.根据oracle的转换和分区规则列出 oracle_partition - hive_partition 对应关系
			 * 4.在target表中创建分区 5.根据oracle_partition - hive_partition
			 * 对应关系把数据导入目标表
			 */

			String mid_table_name = aug_info.getMid_table_name();
			String table_name = aug_info.getTable_name();
			String partition_field = aug_info.getPartition_field();
			String partition_fun = aug_info.getPartition_fun();
			System.out.println(partition_fun);
			String sql = "select " + partition_fun + " par from " + mid_table_name + " group by par ";
			System.out.println("sql:" + sql);
			List<String> par_list = new ArrayList<String>();
			ResultSet rs = hive_conn.execute2(sql);
			String par_value;
			while (rs.next()) {
				par_value = rs.getString(1);
				System.out.println("partition_value:" + par_value);
				par_list.add(par_value);
			}
			rs.close();
			/**
			 * 创建分区 执行插入语句
			 */
			String insert_sql = "";
			for (String partition_value : par_list) {
				hive_conn.execute3(" alter table " + table_name + " add if not exists partition(p_date='"
						+ partition_value + "')");
				insert_sql = "insert into table " + table_name + " partition ( p_date='" + partition_value
						+ "' ) select * from " + mid_table_name + " where " + partition_fun + " = '" + partition_value
						+ "' ";

				try {
					hive_conn.execute3(insert_sql);
				} catch (Exception e) {
					System.out.println("Exception:" + e.toString());
					System.out.println("insert_exception:" + insert_sql);
					/**********************
					 * new add [BEGIN]********************
					 * 
					 * @author CHENGKAI.SHENG
					 * @since 2016-12-16
					 * 
					 */
					FileLoadingProgress.updateJobFinalStatus(source_bean_list, "", e, "A", "中间表  >>  目标表\n\t" +
								insert_sql + "\n\t" + e.getMessage(), 4);
					throw e;
					//hive_conn.execute3(insert_sql);
				}
			}
			
			FileLoadingProgress.updateJobProgress(source_bean_list, 100);
			FileLoadingProgress.updateJobFinalStatus(source_bean_list, "成功", null, "A", "加载结束", 2);
		}
		else {
			FileLoadingProgress.updateJobProgress(source_bean_list, 100);
			FileLoadingProgress.updateJobFinalStatus(source_bean_list, "成功", null, "A", "加载结束", 2);
		}
		return file_date;
	}
}