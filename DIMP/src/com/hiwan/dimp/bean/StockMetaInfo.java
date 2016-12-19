package com.hiwan.dimp.bean;

import java.util.List;
import java.util.Map;

public class StockMetaInfo extends Object implements Cloneable{
	private String   md_id;
	private String   table_name;
	private String   field_num;
	private String   row_num;
	private String   stored_size;
	private String   is_partition;
	private String   partition_field;
	private String   table_type;
	private String   business_type;
	private String   developer;
	private String   target_table;
	private String   target_type;
	private String   create_script_hbase;
	private String   create_script_hive;
	private String   status;
	private String   lst_modify_date;
	private String   clear_date;
	private String   primary_key;
	private String   bucket;//分桶字段
	
	private String   st_id;
	private String   table_id;
	private String   add_partition_script;
	private String   import_script;
	private String 	 partition_value;
	private String   st_lst_modify_date;
	private String   st_status ;
	private List<StockMetaInfo> list =null;
	private  List<Map<String, Object>> partitionMaplist =null;
	
	public StockMetaInfo clone()
	{
		try {
			return (StockMetaInfo)super.clone();
		} catch (CloneNotSupportedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	public String getMd_id() {
		return md_id;
	}
	public void setMd_id(String md_id) {
		this.md_id = md_id;
	}
	public String getTable_name() {
		return table_name;
	}
	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}
	public String getField_num() {
		return field_num;
	}
	public void setField_num(String field_num) {
		this.field_num = field_num;
	}
	public String getRow_num() {
		return row_num;
	}
	public void setRow_num(String row_num) {
		this.row_num = row_num;
	}
	public String getStored_size() {
		return stored_size;
	}
	public void setStored_size(String stored_size) {
		this.stored_size = stored_size;
	}
	public String getIs_partition() {
		return is_partition;
	}
	public void setIs_partition(String is_partition) {
		this.is_partition = is_partition;
	}
	public String getPartition_field() {
		return partition_field;
	}
	public void setPartition_field(String partition_field) {
		this.partition_field = partition_field;
	}
	public String getTable_type() {
		return table_type;
	}
	public void setTable_type(String table_type) {
		this.table_type = table_type;
	}
	public String getBusiness_type() {
		return business_type;
	}
	public void setBusiness_type(String business_type) {
		this.business_type = business_type;
	}
	public String getDeveloper() {
		return developer;
	}
	public void setDeveloper(String developer) {
		this.developer = developer;
	}
	public String getTarget_table() {
		return target_table;
	}
	public void setTarget_table(String target_table) {
		this.target_table = target_table;
	}
	public String getTarget_type() {
		return target_type;
	}
	public void setTarget_type(String target_type) {
		this.target_type = target_type;
	}
	public String getCreate_script_hbase() {
		return create_script_hbase;
	}
	public void setCreate_script_hbase(String create_script_hbase) {
		this.create_script_hbase = create_script_hbase;
	}
	public String getCreate_script_hive() {
		return create_script_hive;
	}
	public void setCreate_script_hive(String create_script_hive) {
		this.create_script_hive = create_script_hive;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getLst_modify_date() {
		return lst_modify_date;
	}
	public void setLst_modify_date(String lst_modify_date) {
		this.lst_modify_date = lst_modify_date;
	}
	public String getClear_date() {
		return clear_date;
	}
	public void setClear_date(String clear_date) {
		this.clear_date = clear_date;
	}
	public String getSt_id() {
		return st_id;
	}
	public void setSt_id(String st_id) {
		this.st_id = st_id;
	}
	public String getTable_id() {
		return table_id;
	}
	public void setTable_id(String table_id) {
		this.table_id = table_id;
	}
	public String getAdd_partition_script() {
		return add_partition_script;
	}
	public void setAdd_partition_script(String add_partition_script) {
		this.add_partition_script = add_partition_script;
	}
	public String getImport_script() {
		return import_script;
	}
	public void setImport_script(String import_script) {
		this.import_script = import_script;
	}
	public String getSt_lst_modify_date() {
		return st_lst_modify_date;
	}
	public void setSt_lst_modify_date(String st_lst_modify_date) {
		this.st_lst_modify_date = st_lst_modify_date;
	}
	public String getSt_status() {
		return st_status;
	}
	public void setSt_status(String st_status) {
		this.st_status = st_status;
	}
 
	public List<StockMetaInfo> getList() {
		return list;
	}
	public void setList(List<StockMetaInfo> list) {
		this.list = list;
	}
	public String getPrimary_key() {
		return primary_key;
	}
	public void setPrimary_key(String primary_key) {
		this.primary_key = primary_key;
	}
	 
	 
	public List<Map<String, Object>> getPartitionMaplist() {
		return partitionMaplist;
	}
	public void setPartitionMaplist(List<Map<String, Object>> partitionMaplist) {
		this.partitionMaplist = partitionMaplist;
	}
	
	public String getPartition_value() {
		return partition_value;
	}
	public void setPartition_value(String partition_value) {
		this.partition_value = partition_value;
	}
	
	public String getBucket() {
		return bucket;
	}
	public void setBucket(String bucket) {
		this.bucket = bucket;
	}
	@Override
	public String toString() {
		return "StockMetaInfo [md_id=" + md_id + ", table_name=" + table_name
				+ ", field_num=" + field_num + ", row_num=" + row_num
				+ ", stored_size=" + stored_size + ", is_partition="
				+ is_partition + ", partition_field=" + partition_field
				+ ", table_type=" + table_type + ", business_type="
				+ business_type + ", developer=" + developer
				+ ", target_table=" + target_table + ", target_type="
				+ target_type + ", create_script_hbase=" + create_script_hbase
				+ ", create_script_hive=" + create_script_hive + ", status="
				+ status + ", lst_modify_date=" + lst_modify_date
				+ ", clear_date=" + clear_date + ", primary_key=" + primary_key
				+ ", bucket=" + bucket + ", st_id=" + st_id + ", table_id="
				+ table_id + ", add_partition_script=" + add_partition_script
				+ ", import_script=" + import_script + ", partition_value="
				+ partition_value + ", st_lst_modify_date="
				+ st_lst_modify_date + ", st_status=" + st_status + ", list="
				+ list + ", partitionMaplist=" + partitionMaplist + "]";
	}
	
	 
	 
	 
	 
	 
}
