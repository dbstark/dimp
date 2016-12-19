package com.hiwan.dimp.hbase;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.hiwan.dimp.db.DBAccess;

public class HBaseTest {
	private static String clientPort ;
	private static String quorum;
	private static String parent;
	static Configuration conf ;
	static {
		Properties prop = new Properties();
		try {
			prop.load(DBAccess.class.getClassLoader().getResourceAsStream("hbase_config.properties"));
			clientPort= prop.getProperty("hbase.zookeeper.property.clientPort");
			quorum= prop.getProperty("hbase.zookeeper.quorum");
			parent= prop.getProperty("zookeeper.znode.parent");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		conf = HBaseConfiguration.create() ;
		conf.set("hbase.zookeeper.property.clientPort", clientPort); 
		conf.set("hbase.zookeeper.quorum", quorum); 
		conf.set("zookeeper.znode.parent", parent);
	}
	
	public ResultScanner getTest(){
		ResultScanner rs = null ;
		HTable ht = null ;
		Scan scan = new Scan() ;
		
		try {
			ht = new HTable(conf, "T00_SAVPRO_INTRST".getBytes()) ;
			rs = ht.getScanner(scan) ;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return rs ;
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		for (Result r : new HBaseTest().getTest()) {
			System.out.println(new String(r.getRow()));
		}
	}

}
