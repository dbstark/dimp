package com.hiwan.dimp.test;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

public class CommandTest {

	public static void main(String[] args) throws Exception {
		/*// String arg[]={"-h","-s","3"};
		//String arg[] = { "-s", "4" };
		 String arg[]={"-t","2"};
		// String arg[]={"-m","25","-s","4","-sc","60"};
		// String tableType="30,63";
		// String[] t=tableType.split(",");
		//List<String> list=Arrays.asList(t);
		//System.out.println(list);
		simple(arg);*/
		
		String command = "ps -ef | grep increment" ;
		String[] cmd = {
				"/bin/sh",
				"-c",
				"ps -ef | grep increment"
				};
		Process process = Runtime.getRuntime().exec(cmd);
		System.out.println(process);
		BufferedInputStream bis = new BufferedInputStream(process.getInputStream());
		BufferedReader br = new BufferedReader(new InputStreamReader(bis));
		System.out.println(br) ;
		String progress = "" ;
		String line = "" ;
		while(( line = br.readLine() ) != null ) {
			progress += line  ;
		}
		System.out.println(progress);
				
	}

	public static void simple(String[] args) throws Exception {
		Map<String,String> argss=new HashMap<String, String>();
		Options options=new Options();
		options.addOption("h","help", false, "Example:java -cp /root/dimp.jar com.hiwan.dimp.service.MainImport -h"); 
    	options.addOption("i","init", true, "Init hive");
    	options.addOption("s","status", true, "Tb_table_metadata status(Integer) default 0"); 
    	options.addOption("n","tableName", true, "TableName(String)");
    	options.addOption("t","tableType", true, "1:xinxi,2:peizhi,3:mingxi,4:huizong. You must be use a comma separated");
    	options.addOption("m","mapreduce", true, "Table mapreduce num(Integer) default 50");
    	options.addOption("sc","splitCount", true, "Table splitCount num(Integer) default 140000");
		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, args);
		HelpFormatter hf = new HelpFormatter();
		argss.put("status", "0");
		argss.put("mr", "25");
		argss.put("splitCount", "50");

		if (cmd.hasOption("i")) {
			System.out.println("init");
		} else if (cmd.hasOption("h")) {
			hf.printHelp("MainImport", options, true);
		} else {
			if (cmd.hasOption("s")) {
				String status = cmd.getOptionValue("s");
				argss.put("status", status == null ? "0" : status);
			}
			if (cmd.hasOption("n")) {
				String tableName = cmd.getOptionValue("n");
				argss.put("tableName",tableName);
			}
			if (cmd.hasOption("m")) {
				String mapreduce = cmd.getOptionValue("m");
				System.out.println(mapreduce);
				//argss.put("mr", mapreduce == null ? "25" : mapreduce);
			}
			if (cmd.hasOption("sc")) {
				String splitCount = cmd.getOptionValue("sc");
				argss.put("splitCount", splitCount == null ? "50" : splitCount);
			}
			if (cmd.hasOption("t")) {
				String tableType = cmd.getOptionValue("t");
				System.out.println(tableType);
				String[] t=tableType.split(",");
				List<String> list=Arrays.asList(t);
				if(list.contains("1")){
					argss.put("xinxi", "信息表");
				}
				if(list.contains("2")){
					argss.put("peizhi", "配置表");
				}
				if(list.contains("3")){
					argss.put("mingxi", "明细表");				
				}
				if(list.contains("4")){
					argss.put("huizong","汇总表");
				}
			}

			System.out.println(argss);
		}
	}
}