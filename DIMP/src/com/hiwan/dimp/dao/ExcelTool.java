package com.hiwan.dimp.dao;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.NumberToTextConverter;

import com.hiwan.dimp.bean.StockMetaInfo;

public class ExcelTool {
	private Workbook wb;
	public ExcelTool(String filePath){
		try {
			InputStream is = new FileInputStream(filePath);
			//创建 POI文件系统对象
			wb = WorkbookFactory.create(is);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (InvalidFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public  Map<String,StockMetaInfo> readXls() throws Exception {
		Map<String,StockMetaInfo> maps=new HashMap<String,StockMetaInfo>();
		StockMetaInfo smi=null;
        //获取工作薄
        Sheet sheet = wb.getSheetAt(0);
        int rows=sheet.getPhysicalNumberOfRows();
        for (int i = 1; i <rows ; i++) {
        	Row row=sheet.getRow(i);
        	smi=new StockMetaInfo();
        	
        	String tableName=getCellStringValue(row.getCell(0));
        	smi.setTable_name(tableName);
        	String tableType=getCellStringValue(row.getCell(1));
        	smi.setTable_type(tableType);
        	String businessType=getCellStringValue(row.getCell(2));
        	smi.setBusiness_type(businessType);
			
        	maps.put(tableName, smi);
		}
        return maps;
	}
	public StockMetaInfo queryTableInfo(String tableName){
		StockMetaInfo smi = null;
		try {
			smi = readXls().get(tableName);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return smi;
	}

	public String getCellStringValue(Cell cell) {
		String cellValue = "";
		String dateFormat = "yyyy-MM-dd";
		// System.out.println("当前单元格类型是:"+cell.getCellType());
		if (cell == null) {
			return cellValue;
		}
		switch (cell.getCellType()) {
		case Cell.CELL_TYPE_STRING:// 字符串类型
			cellValue = cell.getStringCellValue().toString();
			if (cellValue.trim().equals("") || cellValue.trim().length() <= 0)
				cellValue = "";
			break;
		case Cell.CELL_TYPE_NUMERIC: // 数值类型(包含日期)
			if (DateUtil.isCellDateFormatted(cell)) {
				Date date = cell.getDateCellValue();
				cellValue = new SimpleDateFormat(dateFormat).format(date);
			} else {
				cellValue = NumberToTextConverter.toText(cell.getNumericCellValue());
			}
			break;
		case Cell.CELL_TYPE_FORMULA: // 公式

			try {
				cellValue = String.valueOf(cell.getStringCellValue());
			} catch (IllegalStateException e) {
				// cellValue = String.valueOf(cell.getNumericCellValue());
				cellValue = NumberToTextConverter.toText(cell.getNumericCellValue());
			}
			break;
		case Cell.CELL_TYPE_BLANK:
			cellValue = "";
			break;
		case Cell.CELL_TYPE_BOOLEAN:
			break;
		case Cell.CELL_TYPE_ERROR:
			break;
		default:
			cellValue = "";
			break;
		}
		return cellValue.trim();
	}

}
