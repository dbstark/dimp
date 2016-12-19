package com.hiwan.dimp.incremental.table;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Processor.update_partition_column_statistics;

import com.google.common.util.concurrent.ExecutionError;
import com.hiwan.dimp.db.StreamGobbler;
import com.hiwan.dimp.incremental.bean.AugmentInfo;
import com.hiwan.dimp.incremental.bean.SourceFileBean;
import com.hiwan.dimp.incremental.dao.FileLoadingProgress;
import com.hiwan.dimp.incremental.util.HdfsFileSystem;
import com.hiwan.dimp.incremental.util.HiveConnection2;

public class InfoTable {

	/**
	 * 信息表
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws SQLException
	 */
	public static boolean config_table_proces(AugmentInfo aug_info, String file_path, HiveConnection2 hive_conn)
			throws Exception {

		/**
		 * 1.清空中间表 2.load文件进入中间表 3.判断信息表是否是分区表 是:创建分区,merge数据到分区(orc表是否需要先创建分区)
		 * 否:直接进行merge
		 */
		hive_conn.execute(" truncate table " + aug_info.getMid_table_name());
		hive_conn.execute(" truncate table " + aug_info.getTemp_table_name());

		String load_shell = aug_info.getAddfile_hdfs_script().toLowerCase().replace("data_file_path", file_path);
		Process process = Runtime.getRuntime().exec(load_shell);
		StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), "ERROR", " ");
		errorGobbler.start();
		StreamGobbler outGobbler = new StreamGobbler(process.getInputStream(), "STDOUT", " ");
		outGobbler.start();
		process.waitFor();

		/**
		 * 对数据进行去重 根据主键进行判断,主键重复的情况下只取第一条数据 创建temp表和mid表的结构相一致
		 * 增量导入表添加一列：有temp到mid的sql语句 去重的sql语句 sql语句的编写：
		 */
		hive_conn.execute(aug_info.getTemp_to_mid());

		String import_script = aug_info.getImport_script();
		// String partition_value = "" ;

		boolean where = true;
		if ("1".equals(aug_info.getIs_partition())) {
			/**
			 * 判断放入哪个分区
			 */
			String mid_table_name = aug_info.getMid_table_name();
			String table_name = aug_info.getTable_name();
			String partition_field = aug_info.getPartition_field();
			String partition_fun = aug_info.getPartition_fun();
			String sql = "select " + partition_fun + " par from " + mid_table_name + " group by par ";
			// System.out.println(sql);
			ResultSet rs = hive_conn.execute2(sql);
			List<String> par_list = new ArrayList<String>();
			String par_value;
			while (rs.next()) {
				par_value = rs.getString(1);
				System.out.println("partition_value:" + par_value);
				par_list.add(par_value);
			}
			for (String partition_value : par_list) {
				// System.out.println(" alter table " + table_name + " add if
				// not exists partition(p_date='" + partition_value + "')");
				hive_conn.execute(" alter table " + table_name + " add if not exists partition(p_date='"
						+ partition_value + "')");
				// hive_conn.execute("insert into " + table_name + " partition
				// (pdate=" + partition_value + ") select * from " +
				// mid_table_name + " where " + partition_fun + " = " +
				// partition_value) ;
				import_script = import_script
						.replace("partition_value", "partition( p_date='" + partition_value + "' )")
						.replace(mid_table_name, "( select * from " + mid_table_name + " where " + partition_fun
								+ " = '" + partition_value + "' ) ");
				// System.out.println("import_script:"+import_script);
				where = hive_conn.execute(import_script);
			}
		} else {
			where = hive_conn.execute(import_script.replace("partition_value", ""));
		}

		hive_conn.close();
		return where;
	}

	public static boolean config_table_proces(AugmentInfo aug_info, List<SourceFileBean> source_bean_list,
			HiveConnection2 hive_conn) throws Exception {

		/**
		 * 1.清空中间表 2.load文件进入中间表 3.判断信息表是否是分区表 是:创建分区,merge数据到分区(orc表是否需要先创建分区)
		 * 否:直接进行merge
		 */
		hive_conn.execute(" truncate table " + aug_info.getMid_table_name());
		hive_conn.execute(" truncate table " + aug_info.getTemp_table_name());

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

		/**
		 * 对数据进行去重 根据主键进行判断,主键重复的情况下只取第一条数据 创建temp表和mid表的结构相一致
		 * 增量导入表添加一列：有temp到mid的sql语句 去重的sql语句 sql语句的编写：
		 */
		hive_conn.execute(aug_info.getTemp_to_mid());

		String import_script = aug_info.getImport_script();
		// String partition_value = "" ;

		boolean where = true;
		if ("1".equals(aug_info.getIs_partition())) {
			/**
			 * 判断放入哪个分区
			 */
			String mid_table_name = aug_info.getMid_table_name();
			String table_name = aug_info.getTable_name();
			String partition_field = aug_info.getPartition_field();
			String partition_fun = aug_info.getPartition_fun();
			String sql = "select " + partition_fun + " par from " + mid_table_name + " group by par ";
			// System.out.println(sql);
			ResultSet rs = hive_conn.execute2(sql);
			List<String> par_list = new ArrayList<String>();
			String par_value;
			while (rs.next()) {
				par_value = rs.getString(1);
				System.out.println("partition_value:" + par_value);
				par_list.add(par_value);
			}
			for (String partition_value : par_list) {
				// System.out.println(" alter table " + table_name + " add if
				// not exists partition(p_date='" + partition_value + "')");
				hive_conn.execute(" alter table " + table_name + " add if not exists partition(p_date='"
						+ partition_value + "')");
				// hive_conn.execute("insert into " + table_name + " partition
				// (pdate=" + partition_value + ") select * from " +
				// mid_table_name + " where " + partition_fun + " = " +
				// partition_value) ;
				import_script = import_script
						.replace("partition_value", "partition( p_date='" + partition_value + "' )")
						.replace(mid_table_name, "( select * from " + mid_table_name + " where " + partition_fun
								+ " = '" + partition_value + "' ) ");
				// System.out.println("import_script:"+import_script);
				where = hive_conn.execute(import_script);
			}
		} else {
			where = hive_conn.execute(import_script.replace("partition_value", ""));
		}

		// hive_conn.close() ;
		return where;
	}

	/**
	 * 功 能： 数据流： 实 现：
	 * 
	 * @param aug_info
	 * @param source_bean_list
	 * @param hive_conn
	 * @param hfs
	 * @return
	 * @throws Exception
	 */
	public static String config_table_proces(AugmentInfo aug_info, List<SourceFileBean> source_bean_list,
			HiveConnection2 hive_conn, HdfsFileSystem hfs) throws Exception {

		/**
		 * 1.清空中间表 2.load文件进入中间表 3.判断信息表是否是分区表 是:创建分区,merge数据到分区(orc表是否需要先创建分区)
		 * 否:直接进行merge
		 */
		try {
			hive_conn.execute3(" truncate table " + aug_info.getMid_table_name());
			hive_conn.execute3(" truncate table " + aug_info.getTemp_table_name());
			FileLoadingProgress.updateJobProgress(source_bean_list, 25);
		} catch (Exception e) {
			FileLoadingProgress.updateJobFinalStatus(source_bean_list, "异常", e, "U", "清空中间表、临时表\n\t" + 
									e.getMessage(), 4);
			throw e;
		}

		String old_date = "";
		String file_date = null;
		String[] f_arr = null;
		String fileName = null;

		for (SourceFileBean source_bean : source_bean_list) {
			fileName = source_bean.getSource_path();

			f_arr = source_bean.getSource_path().split("_");
			file_date = f_arr[f_arr.length - 3];
			if (file_date.compareTo(old_date) > 0) {
				old_date = file_date;
			}
			String load_shell = aug_info.getAddfile_hdfs_script().toLowerCase();
			String file_code_change_path = source_bean.getMid_source_path();
			String hdfs_path = hive_conn.table_hdfs_path(aug_info.getTemp_table_name());
			// hfs.put_file_to_hdfs(file_code_change_path, load_shell,aug_info)
			// ;
			try {
				hfs.put_file_to_hdfs2(file_code_change_path, hdfs_path);
				//加载正常，未结束
				FileLoadingProgress.updateFileProgress(fileName, 55);
			} catch (Exception e) {
				//加载异常
				FileLoadingProgress.updateJobFinalStatus(source_bean_list, "异常", e, "U", "本地文件  >> 临时表\n\t" + 
									e.getMessage(), 4);
				throw e;
			}
		}

		file_date = old_date;
		/**
		 * 对数据进行去重 根据主键进行判断,主键重复的情况下只取第一条数据 创建temp表和mid表的结构相一致
		 * 增量导入表添加一列：有temp到mid的sql语句 去重的sql语句 sql语句的编写：
		 */
		try {
			hive_conn.execute3(aug_info.getTemp_to_mid());
			//加载正常, 未结束
			FileLoadingProgress.updateJobProgress(source_bean_list, 65);
		} catch (Exception e) {
			//加载异常
			FileLoadingProgress.updateJobFinalStatus(source_bean_list, "异常", e, "U", "临时表  >> 中间表\n\t" + 
								e.getMessage(), 4);
			throw e;
		}

		String import_script = aug_info.getImport_script().replace("1 = 1  and", "");
		String import_sql = "";
		if ("1".equals(aug_info.getIs_partition())) {
			/**
			 * 判断放入哪个分区
			 */
			String mid_table_name = aug_info.getMid_table_name();
			String table_name = aug_info.getTable_name();
			String partition_field = aug_info.getPartition_field();
			String partition_fun = aug_info.getPartition_fun();
			String sql = "select " + partition_fun + " par from " + mid_table_name + " group by par ";
			// System.out.println(sql);
			List<String> par_list = new ArrayList<String>();
			ResultSet rs = hive_conn.execute2(sql);
			String par_value;
			while (rs.next()) {
				par_value = rs.getString(1);
				System.out.println("partition_value:" + par_value);
				par_list.add(par_value);
			}
			rs.close();
			for (String partition_value : par_list) {
				hive_conn.execute3(" alter table " + table_name + " add if not exists partition(p_date='"
						+ partition_value + "')");
				import_sql = import_script.replace("partition_value", "partition( p_date='" + partition_value + "' )")
						.replace(mid_table_name, "( select * from " + mid_table_name + " where " + partition_fun
								+ " = '" + partition_value + "' ) ");
				try {
					hive_conn.execute3(import_sql);
				} catch (Exception e) {
					System.out.println("Exception:" + e.toString());
					System.out.println("merge_exception:" + import_sql);
		//			hive_conn.execute3(import_sql);
					//加载异常
					FileLoadingProgress.updateJobFinalStatus(source_bean_list, "异常", e, "U", "中间表   >> 目标表对应分区\n\t" +
											e.getMessage(), 4);
					throw e;
				}
			}
			// 2016-12-19	
			FileLoadingProgress.updateJobProgress(source_bean_list, 100);
			FileLoadingProgress.updateJobFinalStatus(source_bean_list, "成功", null, "U", "中间表  >> 目标表对应分区 ", 2);	
		} else {
			import_sql = import_script.replace("partition_value", "");
			try {
				hive_conn.execute3(import_sql);
				FileLoadingProgress.updateJobProgress(source_bean_list, 100);
				FileLoadingProgress.updateJobFinalStatus(source_bean_list, "成功", null, "U", "中间表  >> 目标表", 2);
			} catch (Exception e) {
				System.out.println("Exception:" + e.toString());
				System.out.println("merge_exception:" + import_sql);
				//hive_conn.execute3(import_sql);
				FileLoadingProgress.updateJobFinalStatus(source_bean_list, "异常", e, "U", "中间表  >> 目标表(merge)\n\t" + import_script, 4);
				throw e;
			}
		}
		return file_date;
	}
}