package com.hiwan.dimp.test;

import java.sql.DriverManager;
import java.util.Vector;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Driver;

public class ConnectionPool {

	private String jdbcDriver = "" ;
	private String dbIrl = "" ;
	private String dbUsername = "" ;
	private String dbPassword = "" ;
	private String testTable = "" ;
	private int initialConnections = 10 ;
	private int incrementalConnections = 5 ;
	private int maxConnections = 50 ;
	private Vector<PooledConnection> connections = null ;
	
	public ConnectionPool(String jdbcDriver, String dbIrl, String dbUsername,
			String dbPassword) {
		super();
		this.jdbcDriver = jdbcDriver;
		this.dbIrl = dbIrl;
		this.dbUsername = dbUsername;
		this.dbPassword = dbPassword;
	}

	public synchronized void createPool() throws Exception {
		
		if(connections != null){
			return ;
		}
		Driver driver = (Driver) Class.forName(this.jdbcDriver).newInstance() ;
		DriverManager.registerDriver(driver) ;
		connections = new Vector<PooledConnection>() ;
		createConnections(this.initialConnections) ;
		System.out.println("数据库连接池创建成功");
	}
	
	public void createConnections( int numConnections ){
		
		for(int i = 0 ; i < numConnections ; i++){
			if(this.maxConnections > 0 && this.connections.size() >= this.maxConnections){
				break ;
			}
			
		}
		
	}
	
	
	public Vector<PooledConnection> getConnections() {
		return connections;
	}

	public void setConnections(Vector<PooledConnection> connections) {
		this.connections = connections;
	}

	public int getInitialConnections() {
		return initialConnections;
	}

	public void setInitialConnections(int initialConnections) {
		this.initialConnections = initialConnections;
	}

	public int getIncrementalConnections() {
		return incrementalConnections;
	}

	public void setIncrementalConnections(int incrementalConnections) {
		this.incrementalConnections = incrementalConnections;
	}

	public int getMaxConnections() {
		return maxConnections;
	}

	public void setMaxConnections(int maxConnections) {
		this.maxConnections = maxConnections;
	}

	
	class PooledConnection{
		
		Connection connection = null ;
		boolean busy = false ;
		
		public PooledConnection(Connection connection) {
			super();
			this.connection = connection;
		}
		public Connection getConnection() {
			return connection;
		}
		public void setConnection(Connection connection) {
			this.connection = connection;
		}
		public boolean isBusy() {
			return busy;
		}
		public void setBusy(boolean busy) {
			this.busy = busy;
		}
		
	}
	
}
