package samar.hive.client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveTutorialJdbcClient {
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			System.exit(1);
		}
		Connection con = DriverManager.getConnection("jdbc:hive://192.168.2.6:10000/default", "", "");
		Statement stmt = con.createStatement();
		ResultSet resultSet = stmt.executeQuery("select * from comp_adhar");
		while (resultSet.next()) {

			System.out.println("Hash : " + resultSet.getString("hash"));
			System.out.println("Category : " + resultSet.getString("category"));
		}
	}
}
