package com.hiwan.dimp.incremental.util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.channels.FileChannel;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.hiwan.dimp.incremental.bean.AugIncrMapInfo;
import com.hiwan.dimp.incremental.bean.AugmentInfo;
import com.hiwan.dimp.incremental.bean.FileDetailsBean;
import com.hiwan.dimp.incremental.bean.IncrDetailLog;
import com.hiwan.dimp.incremental.bean.IncrementDate;
import com.hiwan.dimp.incremental.bean.SourceFileBean;
import com.hiwan.dimp.incremental.dao.CtlInfoDao;
import com.hiwan.dimp.incremental.dao.FileDetailsDao;
import com.hiwan.dimp.incremental.dao.FileLoadingProgress;
import com.hiwan.dimp.incremental.dao.ProgressRecord;

/**
 * 获取数据文件夹,list下面的所有文件,并且把完成的数据文件mv到数据仓库中
 */
public class FileListUtil {

	/**
	 * list file
	 */
	public List<String> list_data_file(String path) {
		List<String> list_file = new ArrayList<String>();
		list_file = getFile(path, list_file);
		return list_file;
	}

	/**
	 * 
	 * @param source_path,
	 *            下发增量文件所在目录，eg. /home/jshdata_increment/data/source
	 * @param job_table_map,
	 *            job（数据源）和对应table的映射, 1个table对应n个job（n个数据源）
	 * @param other_path，表不存在时候，数据文件移动到的目录
	 * @param abnormal_path,
	 *            <异常目录？>
	 * @param aug_info_map,
	 *            <??>
	 * @param incre_date_map,
	 *            获得job-IncrementDate的map
	 * @return AugIncrMapInfo,
	 * @throws Exception
	 */
	public AugIncrMapInfo map_data_file(String source_path, // 源文件目录
			Map<String, String> job_table_map, // job和table映射
			String other_path, // 表不存在的时候数据存放目录
			String abnormal_path, // 异常目录
			Map<String, AugmentInfo> aug_info_map, // <??>
			Map<String, IncrementDate> incre_date_map // <??>
	) throws Exception {

		CtlInfoDao ctl_info_dao = new CtlInfoDao(); // job名称及对应的文件个数
		Map<String, IncrDetailLog> incr_detail_map = new HashMap<String, IncrDetailLog>();
		IncrDetailLog incr_detail_log = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		// key:job名称 value:job对应的文件列表
		Map<String, List<SourceFileBean>> map_file = new HashMap<String, List<SourceFileBean>>();

		Map<String, String> time_map = new HashMap<String, String>();
		/** job -- table* */
		// stage [6]
		// 获取job--files
		/*
		 * map_file key1 - list1 |- SourceFileBean1 |- SourceFileBean1 |-...
		 * key2 - list2 |- SourceFileBean2 |- SourceFileBean2 |-... ... - ...
		 */
		map_file = getFile(source_path, map_file, job_table_map, other_path, aug_info_map, incre_date_map, time_map);

		// 对map_file进行转码
		String right_wrong = "";
		Map<String, List<SourceFileBean>> map_file2 = new HashMap<String, List<SourceFileBean>>(map_file);

		String cim_job_name = "";
		String table_name = "";
		AugmentInfo aug_info = null;
		FileDetailsBean fdb = null;
		int column_num = 0;
		
		/***************new add [BEGIN]**************
		 * @author CHENGKAI.SHENG
		 * @since 2016-12-15 
		 */
		FileLoadingProgress.connect2Mysql();

		/***************new add [END]****************/

		for (Entry<String, List<SourceFileBean>> entry : map_file2.entrySet()) {
			cim_job_name = entry.getKey();
			List<SourceFileBean> sb_list = entry.getValue();

			if (job_table_map.containsKey(cim_job_name)) {
				table_name = job_table_map.get(cim_job_name);
			}
			aug_info = aug_info_map.get(table_name);
			String file_date = sb_list.get(0).getFile_date();
			String job_date = cim_job_name + "_" + file_date;
			// 获取当前job及日期对应的文件个数
			int num = ctl_info_dao.get_ctl_info(job_date.toLowerCase()).getFile_num();
			if (num != 0 && num != sb_list.size()) {
				map_file.remove(cim_job_name);
			} else if (aug_info instanceof AugmentInfo) {
				column_num = aug_info.getCreate_script_hive().split(",").length;
				String table_type_ = aug_info.getTable_type();
				boolean status = false;
				String error_message = "";
				String file_list = "";
				incr_detail_log = new IncrDetailLog();
				if ("配置表".equals(table_type_)) {
					incr_detail_log.setJob_type("F");
				} else if ("信息表".equals(table_type_)) {
					incr_detail_log.setJob_type("U");
				} else {
					incr_detail_log.setJob_type("A");
				}
				incr_detail_log.setJob_name(cim_job_name);
				incr_detail_log.setTable_name(table_name);
				incr_detail_log.setFile_date(file_date);
				incr_detail_log.setFile_num(sb_list.size());
				incr_detail_log.setBegintime(sdf.format(new Date()));

				/**
				 * [a file]
				 */
				for (SourceFileBean sfb : sb_list) {
					/****************new add [BEGIN]***************
					 * @author CHENGKAI.SHENG
					 * @since 2016-12-15
					 */
					String fileName = sfb.getSource_path();
					ProgressRecord pr = new ProgressRecord(cim_job_name, fileName, incr_detail_log.getJob_type(),
							1, 1, "PROCESSING", new Date().toString(), null);
					FileLoadingProgress.insertTableFileLoadingProgressValues(pr);

					/****************new add [END]*****************/

					file_list = file_list + sfb.getSource_path() + ";";
					System.out.println("table、job及date：table：" + table_name + "\tjob_name:" + cim_job_name + "\tdate:"
							+ sfb.getFile_date());
					// create_directory(abnormal_path+"/" + sfb.getFile_date())
					// ;
					// [stage x] 转码
					fdb = FileDetailsDao.obtain_file_details(column_num, sfb.getSource_path(),
							abnormal_path + "/" + sfb.getFile_date());
					right_wrong = fdb.getRight_wrong();
					
					/****************new add [BEGIN]***************
					 * @author CHENGKAI.SHENG
					 * @since 2016-12-15
					 */
					if ("right".equals(right_wrong)) {
						FileLoadingProgress.updateProgress(fileName, 10);
					}

					/****************new add [END]*****************/

					if ("10".equals(aug_info.getStatus())) {

					} else {
						if ("error".equals(right_wrong)) {
							String message = "error:解密错误文件:cim_job_name:" + entry.getKey() + "\ttable_name:"
									+ job_table_map.get(entry.getKey()) + "\tfile_name:" + sfb.getSource_path();
							System.err.println(message);
							status = true;
							error_message = error_message + message + ";";
							
							/*******************new add [BEGIN]****************
							 * @author CHEGNKAI.SHENG
							 * @since 2016-12-16
							 */
							FileLoadingProgress.updateFinalStatus(fileName, 
									new Date().toString(), 4, "[Message]: ERROR\n\t" + error_message +
									"[Stage]: TRANSCODING");

							/*******************new add [END]******************/
						}
						if (fdb.getAbnormal_proportion() > 0.1) {
							String message = "error:字段异常文件:cim_job_name:" + entry.getKey() + "\tcolumn_num:"
									+ column_num + "\ttable_name:" + job_table_map.get(entry.getKey()) + "\tfile_name:"
									+ sfb.getSource_path();
							System.err.println(message);
							file_mv3(entry.getValue(), "/home/jshdata_increment/data/abnormal/source");
							status = true;
							error_message = error_message + message + ";";

							/*******************new add [BEGIN]****************
							 * @author CHEGNKAI.SHENG
							 * @since 2016-12-16
							 */
							FileLoadingProgress.updateFinalStatus(fileName, 
									new Date().toString(), 4, error_message);

							/*******************new add [END]******************/
						}
					}
				}
				// 判断status和增量导入中表的状态status，用于判断是否需要处理
				incr_detail_log.setEndtime(sdf.format(new Date()));
				incr_detail_log.setFile_list(file_list);
				incr_detail_log.setMark(error_message);
				incr_detail_log.setSdatastatus("3");
				incr_detail_log.setPdatastatus("3");
				if (status) {
					map_file.remove(cim_job_name);
					incr_detail_log.setConvertstatus("1");
					incr_detail_map.put(cim_job_name, incr_detail_log);
					// break ;
				} else {
					incr_detail_log.setConvertstatus("0");
					incr_detail_map.put(cim_job_name, incr_detail_log);
				}
			} else {
				create_directory(other_path + "/" + file_date);
				file_mv3(map_file.get(cim_job_name), other_path + "/" + file_date);
				map_file.remove(entry.getKey());
			}
		}
		ctl_info_dao.close();
		return new AugIncrMapInfo(incr_detail_map, map_file);
		// return map_file ;
	}

	public List<String> getFile(String path, List<String> list) {
		File file = new File(path);
		File[] arr_file = file.listFiles();
		for (File f : arr_file) {
			if (f.isFile()) {
				// 对文件名和后缀名的格式进行判断
				String file_path = f.getPath();
				String file_name = f.getName();
				if (file_name.indexOf("3200") > -1 && file_name.split("\\.").length > 1
						&& "dat".equals(file_name.split("\\.")[1])) {
					file_code_change(file_path);
					list.add(file_path);
				}
			} else {
				getFile(f.getPath(), list);
			}
		}
		return list;
	}

	/**
	 * 
	 * @param path,
	 *            The path where source files locate
	 * @param map,
	 * @param job_table_map
	 * @param other_path
	 * @param aug_info_map
	 * @param incre_date_map
	 * @param time_map
	 * @return
	 */
	public Map<String, List<SourceFileBean>> getFile(String path, Map<String, List<SourceFileBean>> map,
			Map<String, String> job_table_map, String other_path, Map<String, AugmentInfo> aug_info_map,
			Map<String, IncrementDate> incre_date_map, Map<String, String> time_map) {

		File file = new File(path);
		File[] arr_file = file.listFiles();
		if (arr_file == null || arr_file.length == 0) { // 判断source目录下是否有需要操作的数据文件
			System.out.println("没有文件需要加载");
		} else {
			for (File f : arr_file) {
				if (f.isFile()) {
					// 对文件名和后缀名的格式进行判断
					String file_path = f.getPath(); // 文件路径
					String file_name = f.getName(); // 文件名
					// 时间判断,判断文件最后修改时间是否符合要求: 当前时间和最后修改时间相差10分钟
					if (System.currentTimeMillis() - f.lastModified() > 10 * 60 * 1000) {
						// 后缀名判断,如果以_MID.dat结尾就进行删除操作
						if (!(file_name.endsWith("_MID.dat"))) {
							List<SourceFileBean> list = null;
							// 文件名包含"_","3200",文件格式为dat
							if (file_name.indexOf("_") > -1 && file_name.indexOf("3200") > -1
									&& file_name.split("\\.").length > 1 && "dat".equals(file_name.split("\\.")[1])) {
								String job_name = file_name.substring(0, file_name.indexOf("3200") - 1).toUpperCase(); // 文件名切分,获取job名称
								String table_name = "不存在";
								IncrementDate incre_date = null; // 增量导入对象
								if (job_table_map.containsKey(job_name)) {
									table_name = job_table_map.get(job_name); // 根据job名称获取对应的table名称
								}
								AugmentInfo aug_info = aug_info_map.get(table_name); // 获取表对应的增量导入名称
								incre_date = incre_date_map.get(job_name); // 获取增量导入信息对象
								String[] f_arr = file_name.split("_");
								String file_date = f_arr[f_arr.length - 3]; // 文件名切分获取数据文件日期

								/**
								 * job_name -> table_name -> aug_info ->
								 * table_type
								 */
								if (aug_info instanceof AugmentInfo) {
									// [table] 信息表
									if ("信息表".equals(aug_info.getTable_type())) { // 信息表情况
										if (incre_date instanceof IncrementDate) {
											// < case 1> when file_date =
											// incr_date + 1
											if (file_date.compareTo(evaluate(incre_date.getFile_date())) == 0) {
												if (map.containsKey(job_name)) {
													list = map.get(job_name);
													list.add(new SourceFileBean(file_date, file_path,
															file_name_change(file_path)));
													map.put(job_name, list);
												} else {
													list = new ArrayList<SourceFileBean>();
													list.add(new SourceFileBean(file_date, file_path,
															file_name_change(file_path)));
													map.put(job_name, list);
												}
											}
										} else {
											//
											if (map.containsKey(job_name)) {
												list = map.get(job_name);
												String old_time = time_map.get(job_name);
												// <case 2>: when file_date =
												// old_time
												if (file_date.equals(old_time)) {
													list.add(new SourceFileBean(file_date, file_path,
															file_name_change(file_path)));

													// <case 3>: when file_date
													// >
													// old_time，在old_time之后，直接跳过
												} else if (file_date.compareTo(old_time) > 0) {
													continue;
													// <case 4>: when file_date
													// < old_time, 在old_time之前,
												} else {
													list.clear();
													list.add(new SourceFileBean(file_date, file_path,
															file_name_change(file_path)));
													time_map.put(job_name, file_date);
												}
												map.put(job_name, list);
											} else {
												list = new ArrayList<SourceFileBean>();
												list.add(new SourceFileBean(file_date, file_path,
														file_name_change(file_path)));
												map.put(job_name, list);
												time_map.put(job_name, file_date);
											}
										}
									} else {
										// [table] 配置表、流水表及汇总表
										// 判断文件日期的最早值
										if (map.containsKey(job_name)) {
											list = map.get(job_name);
											String old_time = time_map.get(job_name);
											if (file_date.equals(old_time)) {
												list.add(new SourceFileBean(file_date, file_path,
														file_name_change(file_path)));
											} else if (file_date.compareTo(old_time) > 0) {
												continue;
											} else {
												list.clear();
												list.add(new SourceFileBean(file_date, file_path,
														file_name_change(file_path)));
												time_map.put(job_name, file_date);
											}
											map.put(job_name, list);
										} else {
											list = new ArrayList<SourceFileBean>();
											list.add(new SourceFileBean(file_date, file_path,
													file_name_change(file_path)));
											map.put(job_name, list);
											time_map.put(job_name, file_date);
										}
									}
								} else {
									// [table unkown] 数据文件对应表不存在情况
									System.out.println("目标表不可用：table_name:" + table_name + "\tjob_name:" + job_name);
									// 先根据文件日期进行创建文件夹的操作，然后把文件移动到目标文件夹
									create_directory(other_path + "/" + file_date);
									file_mv(file_path, other_path + "/" + file_date);
								}
							}
						} else {
							// 删除中间文件*_MID.dat
							file_delete(file_path);
						}
					}
				} else {
					// 如果存在目录，递归扫描
					getFile(f.getPath(), map, job_table_map, other_path, aug_info_map, incre_date_map, time_map);
				}
			}
		}
		return map;
	}

	public String evaluate(String date) {
		String result = "";
		try {
			date = date.trim();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			Calendar c = Calendar.getInstance();
			c.setTime(sdf.parse(date));
			int day = c.get(Calendar.DATE);
			c.set(Calendar.DATE, day + 1);
			result = sdf.format(c.getTime());
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * mv file
	 */
	public void file_mv(String file_path, String target_path) {

		try {
			file_channel_copy(file_path, target_path);
			file_delete(file_path);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// file_delete(file_path) ;

	}

	public void file_mv(List<SourceFileBean> source_bean_list, String target_path) {

		try {
			for (SourceFileBean source_bean : source_bean_list) {
				file_channel_copy(source_bean.getMid_source_path(), target_path);
				file_delete(source_bean.getMid_source_path());
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void file_mv3(List<SourceFileBean> source_bean_list, String target_path) {

		try {
			for (SourceFileBean source_bean : source_bean_list) {
				file_channel_copy(source_bean.getSource_path(), target_path);
				file_delete(source_bean.getSource_path());
				file_delete(source_bean.getMid_source_path());
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public String list_to_string(List<SourceFileBean> source_bean_list, String target_path_day) {
		String all_path = "";
		for (SourceFileBean source_bean : source_bean_list) {
			all_path += target_path_day + "/" + new File(source_bean.getMid_source_path()).getName() + ";";
		}
		return all_path;

	}

	/**
	 * copy file
	 * 
	 * @throws Exception
	 */
	public void file_channel_copy(String file_path, String target_path) throws Exception {
		File now_file = new File(file_path);
		File target_file = new File(target_path + "/" + now_file.getName());

		FileInputStream fis = new FileInputStream(now_file);
		FileOutputStream fos = new FileOutputStream(target_file);

		FileChannel in = fis.getChannel();
		FileChannel out = fos.getChannel();

		in.transferTo(0, in.size(), out);

		out.close();
		in.close();
		fos.close();
		fis.close();

	}

	public static String file_name_change(String file_path) {
		String[] arr = file_path.split("\\.");
		String out_path = arr[0] + "_MID." + arr[1];
		return out_path;
	}

	/**
	 * 转码：把GB2312文件转换为UTF-8文件 并把转换后的文件名称输出出来
	 */
	public static String file_code_change(String file_path) {
		System.out.println("文件转码:" + file_path);
		String[] arr = file_path.split("\\.");
		String out_path = arr[0] + "_MID." + arr[1];
		BufferedReader br;
		BufferedWriter bw;
		String line = "";
		String more_line = "";
		String right_error = "";
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(file_path), "GBK"));
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out_path), "UTF-8"));

			while ((line = br.readLine()) != null) {
				line = line.replace("\r", "").trim().replaceAll("\\s*!\\s*", "!");
				if (line.endsWith("!")) {
					more_line = more_line + line;
					bw.write(line + "\n");
					more_line = "";
				} else {
					more_line = more_line + line;
				}
			}
			if ("".equals(more_line)) {
				right_error = "right";
			} else {
				right_error = "error";
			}
			bw.close();
			br.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return right_error;
	}

	public void create_directory(String path) {
		File file = new File(path);
		if (!file.exists() && !file.isDirectory()) {
			System.out.println(path + ":目录不存在 \n创建");
			file.mkdir();
		} else {
			System.out.println(path + ":目录存在");
		}
	}

	/**
	 * delete file
	 */
	public boolean file_delete(String file_path) {
		boolean flag = false;
		File delete_file = new File(file_path);

		if (!delete_file.exists()) {
			flag = false;
		} else {
			delete_file.delete();
			flag = true;
		}

		return flag;
	}

	public Map<String, String> partition_table_funct(String file_path) throws Exception {
		Map<String, String> map = new HashMap<String, String>();
		File file = new File(file_path);
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line = "";
		String[] table_par_fun;
		while ((line = br.readLine()) != null) {
			line = line.trim();
			table_par_fun = line.split(" ");
			if (table_par_fun.length < 2) {
				continue;
			} else {
				map.put(table_par_fun[0], table_par_fun[1]);
				System.out.println(line);
			}
		}
		br.close();
		return map;
	}

	public static String codeString(String file_path) throws Exception {
		BufferedInputStream br = new BufferedInputStream(new FileInputStream(file_path));
		int p = (br.read() << 8) + br.read();
		String code = null;
		switch (p) {
		case 0xefbb:
			code = "UTF-8";
			break;
		case 0xfffe:
			code = "Unicode";
			break;
		case 0xfeff:
			code = "UTF-16BE";
			break;
		default:
			code = "GBK";
		}
		br.close();
		return code;
	}

}
