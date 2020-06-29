package com.example.dcn;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleStatement;
import oracle.jdbc.dcn.DatabaseChangeEvent;
import oracle.jdbc.dcn.DatabaseChangeListener;
import oracle.jdbc.dcn.DatabaseChangeRegistration;

public class OracleDCN {
	static final String USERNAME = "scott";
	static final String PASSWORD = "tiger";
	static String URL = "jdbc:oracle:thin:@localhost:1521:orcl";

	public static void main(String[] args) {
		OracleDCN oracleDCN = new OracleDCN();
		try {
			oracleDCN.run();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	private void run() throws Exception {
		OracleConnection conn = connect();
		Properties prop = new Properties();
		prop.setProperty(OracleConnection.DCN_NOTIFY_ROWIDS, "true");
		
		DatabaseChangeRegistration dcr = conn.registerDatabaseChangeNotification(prop);
		try {

			dcr.addListener(new DatabaseChangeListener() {

				public void onDatabaseChangeNotification(DatabaseChangeEvent dce) {

					System.out.println("Tablename " + dce.getTableChangeDescription()[0].getTableName()
							+ "Changed row id : "
							+ dce.getTableChangeDescription()[0].getRowChangeDescription()[0].getRowid().stringValue());
				}
			});

			Statement stmt1 = conn.createStatement();
			((OracleStatement) stmt1).setDatabaseChangeRegistration(dcr);
			ResultSet rs1 = stmt1.executeQuery("select * from EMP");

			Statement stmt = conn.createStatement();
			((OracleStatement) stmt).setDatabaseChangeRegistration(dcr);
			ResultSet rs = stmt.executeQuery("select * from BONUS");

			String[] tableNames = dcr.getTables();
			for (int i = 0; i < tableNames.length; i++)
				System.out.println(tableNames[i] + " is part of the registration.");
			while (rs.next()) {
			}
			rs.close();
			stmt.close();
			while (rs1.next()) {
			}
			rs1.close();
			stmt1.close();
		} catch (SQLException ex) {
			if (conn != null) {
				conn.unregisterDatabaseChangeNotification(dcr);
				conn.close();
			}
			throw ex;
		}
	}

	OracleConnection connect() throws SQLException {
		return (OracleConnection) DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:orcl", "scott",
				"scott");
	}
}
