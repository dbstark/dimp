package com.hiwan.dimp.bean;

import java.util.List;

public class StocInfo {
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
	
	private String     interface_code;
	private String     load_method;
	private String     interface_frequency;
	private String     interface_type;
	private String     date_field;
	private String     date_field_typ  ;
	private List<StocInfo> list =null;
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
	 
 
	public String getInterface_code() {
		return interface_code;
	}
	public void setInterface_code(String interface_code) {
		this.interface_code = interface_code;
	}
	public String getLoad_method() {
		return load_method;
	}
	public void setLoad_method(String load_method) {
		this.load_method = load_method;
	}
	public String getInterface_frequency() {
		return interface_frequency;
	}
	public void setInterface_frequency(String interface_frequency) {
		this.interface_frequency = interface_frequency;
	}
	public String getInterface_type() {
		return interface_type;
	}
	public void setInterface_type(String interface_type) {
		this.interface_type = interface_type;
	}
	public String getDate_field() {
		return date_field;
	}
	public void setDate_field(String date_field) {
		this.date_field = date_field;
	}
	public String getDate_field_typ() {
		return date_field_typ;
	}
	public void setDate_field_typ(String date_field_typ) {
		this.date_field_typ = date_field_typ;
	}
	public List<StocInfo> getList() {
		return list;
	}
	public void setList(List<StocInfo> list) {
		this.list = list;
	}
	
	 
	 
	 
}
