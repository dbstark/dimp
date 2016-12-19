package com.hiwan.dimp.incremental.bean;

public class AugmentInfo {

	String aug_id ;
	String table_id ;
	
	String table_name ;
	String table_type ;
	String target_table ;
	String is_partition ;
	String partition_field ;
	String primary_key ;
	String create_script_hive ;
	
	String mid_table_name ;
	String temp_table_name ;
	String create_mid_sql ;
	String create_temp_sql ;
	String temp_to_mid ;
	String addfile_hdfs_script ;
	String duplicate_removal ;
	String import_script ;
	String partition_fun ;
	String lst_modify_date ;
	String status ;
	
	public String getAug_id() {
		return aug_id;
	}
	public void setAug_id(String aug_id) {
		this.aug_id = aug_id;
	}
	public String getTable_id() {
		return table_id;
	}
	public void setTable_id(String table_id) {
		this.table_id = table_id;
	}
	public String getTable_name() {
		return table_name;
	}
	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}
	public String getTable_type() {
		return table_type;
	}
	public void setTable_type(String table_type) {
		this.table_type = table_type;
	}
	public String getTarget_table() {
		return target_table;
	}
	public void setTarget_table(String target_table) {
		this.target_table = target_table;
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
	public String getPrimary_key() {
		return primary_key;
	}
	public void setPrimary_key(String primary_key) {
		this.primary_key = primary_key;
	}
	public String getMid_table_name() {
		return mid_table_name;
	}
	public void setMid_table_name(String mid_table_name) {
		this.mid_table_name = mid_table_name;
	}
	public String getAddfile_hdfs_script() {
		return addfile_hdfs_script;
	}
	public void setAddfile_hdfs_script(String addfile_hdfs_script) {
		this.addfile_hdfs_script = addfile_hdfs_script;
	}
	public String getDuplicate_removal() {
		return duplicate_removal;
	}
	public void setDuplicate_removal(String duplicate_removal) {
		this.duplicate_removal = duplicate_removal;
	}
	public String getImport_script() {
		return import_script;
	}
	public void setImport_script(String import_script) {
		this.import_script = import_script;
	}
	public String getLst_modify_date() {
		return lst_modify_date;
	}
	public void setLst_modify_date(String lst_modify_date) {
		this.lst_modify_date = lst_modify_date;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getCreate_script_hive() {
		return create_script_hive;
	}
	public void setCreate_script_hive(String create_script_hive) {
		this.create_script_hive = create_script_hive;
	}
	public String getPartition_fun() {
		return partition_fun;
	}
	public void setPartition_fun(String partition_fun) {
		this.partition_fun = partition_fun;
	}
	public String getTemp_table_name() {
		return temp_table_name;
	}
	public void setTemp_table_name(String temp_table_name) {
		this.temp_table_name = temp_table_name;
	}
	public String getCreate_mid_sql() {
		return create_mid_sql;
	}
	public void setCreate_mid_sql(String create_mid_sql) {
		this.create_mid_sql = create_mid_sql;
	}
	public String getCreate_temp_sql() {
		return create_temp_sql;
	}
	public void setCreate_temp_sql(String create_temp_sql) {
		this.create_temp_sql = create_temp_sql;
	}
	public String getTemp_to_mid() {
		return temp_to_mid;
	}

	public void setTemp_to_mid(String temp_to_mid) {
		this.temp_to_mid = temp_to_mid;
	}
}