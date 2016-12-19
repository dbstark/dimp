package com.hiwan.dimp.test;

import java.io.IOException;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.junit.Test;

import com.hiwan.dimp.bean.StockMetaInfo;
import com.hiwan.dimp.dao.ExcelTool;

public class ExcelTest {
	@Test
	public void excel() throws InvalidFormatException, IOException{
		String tableName="BB_LOAN_BENEFIT";
		ExcelTool el=new ExcelTool("D:\\Li\\Desktop\\表格-李仕波.xls");
		StockMetaInfo smi= el.queryTableInfo(tableName);
		System.out.println(smi);
	}
}
