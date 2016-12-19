package com.hiwan.dimp.test;

import java.util.HashMap;
import java.util.Map;

import com.hiwan.dimp.bean.StockMetaInfo;
import com.hiwan.dimp.db.SqoopImport;

public class SqoopImportTest {

	public static void main(String[] args) {
		StockMetaInfo smi = new StockMetaInfo();
		
		   smi.setTable_name("T98_INDPTY_PROD_STAT");
			smi.setPrimary_key("CSTM_NO");
			smi.setTable_type("配置表");
			
			Map<String, String> impMap = new HashMap<String, String>();
			/*	
			impMap.put("tableName", "T98_INDPTY_PROD_STAT");
			impMap.put("primaryKey", smi.getPrimary_key());
			impMap.put("num", "4");
			impMap.put("tableType",smi.getTable_type());
			
			SqoopImport.impTableData(impMap);*/
			
			smi.setTable_name("BB_ZD_HH");
			smi.setPrimary_key("DATEDATA,HH");
			smi.setTable_type("信息表");
			
			impMap.put("tableName", "T98_INDPTY_PROD_STAT");
			impMap.put("primaryKey", smi.getPrimary_key());
			impMap.put("num", "4");
			impMap.put("tableType",smi.getTable_type());
			
			 
			
			SqoopImport.impConfigurelAllData(impMap);
			//SqoopImport.impInfoDataPartition(impMap);
			
			/*smi.setTable_name("TB_AST_ASSETTYPE2");
			smi.setPrimary_key("CSTM_NO2");
			smi.setTable_type("信息表");
			smi.setPrimary_key("A_ID,BBB_ID");
			SqoopImport.impTableData(smi);*/
			
			
			

	}

}
