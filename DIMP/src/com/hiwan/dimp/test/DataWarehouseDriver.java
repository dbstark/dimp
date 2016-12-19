/**
 * 
 */
package com.hiwan.dimp.test;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;


/**
 * @author chengkai.sheng
 * @since 2016-09-12
 * @version 1.0
 * 
 *
 */
public class DataWarehouseDriver {
	private String prefix;
	private String suffixSuccess;
	private String suffixFailure;
	
    private String url;            
    private String userName; 
    private String passWord; 
    private String driverClassName;
    
    private Connection con;    
    private Statement stmt;
    
    public DataWarehouseDriver() {
    	prefix = "--> ";
    	suffixSuccess = " [successful] <--\n";	
    	suffixFailure = " [failed] <--\n";	
    	
    	url = "";
    	userName = "";
    	passWord = "";
    	driverClassName = "";
    	
    	con = null;
    	stmt = null;	
	}
    
	public void connectToServer() throws IOException, SQLException, ClassNotFoundException {
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream("./config/hive_config.properties"));
			//prop.load(DataWarehouseDriver.class.getClassLoader().getResourceAsStream("hive_config.properties"));
			
			driverClassName = prop.getProperty("driverClassName");
			Class.forName(driverClassName);
			
			url = prop.getProperty("url");
			userName = prop.getProperty("username");
			passWord = prop.getProperty("password");
			
			System.out.println("url: " + url);
			System.out.println("usrName: " + userName);
			System.out.println("passWord: " + passWord);

			System.out.println("--> connecting to server " + url);
			con = DriverManager.getConnection(url, userName, passWord);
			System.out.println("--> [succesful] connection!");
			stmt = con.createStatement();

		} catch (Exception e) {
			System.out.println("--> [failed] connection!");
			//e.printStackTrace();
			System.exit(-1);
		}
	}

	public void disconnectFromServer() throws SQLException {
		if (stmt != null) {
			try {
				stmt.close();
				stmt = null;
			} catch (SQLException e) {
				//e.printStackTrace();
				throw e;
			}
		}
		if (con != null) {
			try {
				System.out.println("--> disconnecting from server " + url);
				con.close();
				con = null;
				System.out.println("--> [succesful] disconnection!");
			} catch (SQLException e) {
				System.out.println("--> [failed] connection!");
				//e.printStackTrace();	
				throw e;
			}
		}
	}

	public ResultSet executeQuery(String sql) throws SQLException {
		System.out.printf("%s%s", prefix, sql);
		ResultSet res = null;
		try {
			res = stmt.executeQuery(sql);
			System.out.printf("%s", suffixSuccess);
		} catch (SQLException e) {
			System.out.printf("%s", suffixFailure);
			//e.printStackTrace();	
			//System.exit(1);
			throw e;
		}
		return res;
	}

	public void execute(String sql) throws SQLException {
		System.out.printf("%s[executing...] %s", prefix, sql);
		try {
			stmt.execute(sql);
			System.out.printf(" %s", suffixSuccess);
		} catch (SQLException e) {
			System.out.printf(" %s", suffixFailure);
			//e.printStackTrace();			
			//System.exit(1);
			throw e;
		}
	}
	
	public Statement getStatement() {
		return stmt;
	}

	public static void main(String [] args) {
		DataWarehouseDriver inceptor = new DataWarehouseDriver();
		try {
			inceptor.connectToServer();
			
			String sql = null;
			sql = "insert into table src values (9, 'usa')";
			Statement stmt = null;
			stmt = inceptor.getStatement();
			if (stmt.execute(sql)){
				ResultSet rs = stmt.getResultSet();
				while(rs.next()) {
					System.out.println(rs.getString(1));
				}
			}
		}
		catch (Exception e) {
			
		}
	}
}