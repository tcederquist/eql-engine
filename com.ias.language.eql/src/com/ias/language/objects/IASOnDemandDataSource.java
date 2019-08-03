package com.ias.language.objects;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IASOnDemandDataSource {
	private String logs;
	private String name;
	private long rows;
	private List<String> columnNames;
	private List<String> columnLabels;
	private List<Map<String, Object>> data;
	
	public static IASOnDemandDataSource createDataSource() {
		IASOnDemandDataSource dds = new IASOnDemandDataSource();
		dds.columnNames = new ArrayList<String>();
		dds.columnLabels = new ArrayList<String>();
		dds.data = new ArrayList<Map<String,Object>>();
		return dds;
	}
	
	public static String getSafeColumnName(String col) {
		return col
			.replace('#', '_')
			.replace('-', '_')
			.replace('(', '_')
			.replace(')', '_')
			.replace('*', '_')
			.replace('$', '_')
			.replace('.', '_')
			.replace('@', '_')
			.replace('!', '_')
			.replace('%', '_')
			.replace(':', '_')
			.replace(' ', '_')
			;
	}
	
	// Logs come primarily from jsonData calls
	public String getLogs() {
		return logs;
	}
	public void setLogs(String logs) {
		this.logs = logs;
	}
	public List<String> getColumnNames() {
		return columnNames;
	}
	public void setColumnNames(List<String> columnNames) {
		this.columnNames = columnNames;
	}
	public String getName() {
		return name;
	}
	public void setName(String datasetName) {
		this.name = datasetName;
	}
	public List<String> getColumnLabels() {
		return columnLabels;
	}
	public void setColumnLabels(List<String> columnLabels) {
		this.columnLabels = columnLabels;
	}
	public List<Map<String, Object>>getData() {
		return data;
	}
	public void setData(List<Map<String, Object>> rows) {
		this.data = rows;
	}
	public long getRows() {
		return rows;
	}
	public void setRows(long rowCount) {
		this.rows = rowCount;
	}
}
