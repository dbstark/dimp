package com.hiwan.dimp.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.hiwan.dimp.bean.MataStockLog;
import com.hiwan.dimp.bean.StockMetaInfo;
import com.hiwan.dimp.dao.StockMetaInfoDAO;
import com.hiwan.dimp.db.DBAccess;
import com.hiwan.dimp.hive.HiveConnection;
import com.hiwan.dimp.hive.HiveUtils;

public class MetaStoreTest {
	private Connection conn=DBAccess.getConnection_ds_oracle();
	private PreparedStatement psmt = null;
	private ResultSet rs = null;
	/**
	 * 测试分区字段
	 * @throws SQLException
	 */
	//@Test
	public void par() throws SQLException{
		//String tableName="ACC_CLR_JNL";
		String tableName="AG_CUSINFO";
		psmt=conn.prepareStatement("select a.table_name, b.column_name, a.partition_count, a.partitioning_type"
				+ " from USER_PART_TABLES a, USER_PART_KEY_COLUMNS b "
				+ " where 1 = 1 and a.table_name = b.name and a.table_name=?");
		psmt.setString(1, tableName.toUpperCase());
		rs=psmt.executeQuery();
		while(rs.next()){
			System.out.println(rs.getString(2));
		}
	}
	/**
	 * 存量数据的导入
	 * @throws Exception
	 */
	//@Test
	public  void impStockData() throws Exception {		
		StockMetaInfoDAO dao = new StockMetaInfoDAO();
		Map<String, String> map = new HashMap<String, String>();
		map.put("xinxi", "信息表");
		map.put("peizhi", "配置表");
		List<StockMetaInfo> list = dao.getInfoList(map);	
		
		HiveConnection hc = new HiveConnection();		
		MataStockLog msl=null;
		for (StockMetaInfo mt : list) {			
			msl=new MataStockLog();			
			msl.setBegin_time(System.currentTimeMillis()+"");
			//获取hbase
			String hbase = mt.getCreate_script_hbase();			
			String hive = mt.getCreate_script_hive();
			//创建hive
			//hc.execute(hive);
			
			//if(mt.getTable_type()!=null&&"信息表".equals(mt.getTable_type())){
			///	//只有信息表会到hbase  创建hbase脚本
				//HbaseUtils.createTable(hbase);
			//}			 
			 
			//创建分区脚本
		/*	if (mt.getIs_partition().equals("1")) {// '1是分区表，0为不是分区',
				
				List<StockMetaInfo> sp = mt.getList();
				for(StockMetaInfo sc:sp){
					if(sc.getAdd_partition_script()!=null){
						hc.execute(sc.getAdd_partition_script());
					}						
				}								
			}	*/					
			//导入数据 
			//SqoopImport.impTableData(mt);
			//校验数据			
			int source_rownum=Integer.parseInt(mt.getRow_num());
			int target_rownum= new HiveUtils().rowCount(mt.getTable_name());			
			msl.setSource_rownum(source_rownum+"");//源数据条数
			msl.setTarget_rownum(target_rownum+"");//目标数据条数
			int status=(source_rownum!=target_rownum)?0:1;
			msl.setStatus(status+"");			
			msl.setEnd_time(System.currentTimeMillis()+"");			
			//记录完成信息
			logInfo(mt,msl);			
		}
	}
	public void logInfo(StockMetaInfo smi,MataStockLog msl) throws Exception{
		StockMetaInfoDAO.insertLogInfo(smi, msl);
	}
	/**
	 * 测试分区信息
	 */
	@Test
	public void tew(){
		Connection conn = DBAccess.getConnection_ds_oracle();
		PreparedStatement psmt = null;
		ResultSet rs = null;
		List<String> partitions=new ArrayList<String>();
		try {
			psmt = conn.prepareStatement("select table_name,partition_name from USER_TAB_PARTITIONS a where a.table_name=? order by partition_name");
			psmt.setString(1, "TCS_PARTY_AGMT_RELA_H");
			rs = psmt.executeQuery();
			while(rs.next()){
				String partion=rs.getString(2);
				System.out.println(partion);
				partitions.add(partion);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (psmt != null) {
					psmt.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	}
	

}	
