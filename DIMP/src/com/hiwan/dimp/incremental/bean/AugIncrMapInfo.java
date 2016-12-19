package com.hiwan.dimp.incremental.bean;

import java.util.List;
import java.util.Map;

public class AugIncrMapInfo {

	Map<String , IncrDetailLog> incr_detail_map ;
	Map<String,List<SourceFileBean>> map_file ;
	
	public AugIncrMapInfo() {
		super();
	}
	public AugIncrMapInfo(Map<String, IncrDetailLog> incr_detail_map,
			Map<String, List<SourceFileBean>> map_file) {
		super();
		this.incr_detail_map = incr_detail_map;
		this.map_file = map_file;
	}
	
	
	public Map<String, IncrDetailLog> getIncr_detail_map() {
		return incr_detail_map;
	}
	public void setIncr_detail_map(Map<String, IncrDetailLog> incr_detail_map) {
		this.incr_detail_map = incr_detail_map;
	}
	public Map<String, List<SourceFileBean>> getMap_file() {
		return map_file;
	}
	public void setMap_file(Map<String, List<SourceFileBean>> map_file) {
		this.map_file = map_file;
	}
	
	
	
}
