package DB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionHelper {
	
	public static void main(String[] argv) {
		
		try {
			Connection con = getDB2Connection();
			
			java.sql.Statement stmt = con.createStatement();
			 
//            String query = "SELECT COUNT(*) FROM SYSCAT.TABLES";
//            String query = "SELECT COUNT(*) FROM act";
            String query = "SELECT * FROM act";
            java.sql.ResultSet rs = stmt.executeQuery(query);
 
            while (rs.next()) {
//              System.out.println("\n" + query + " = " + rs.getInt(1));
//            	System.out.println(rs.getInt(1));
                System.out.println(rs.getInt(1) + " " + rs.getString(2) + " " + rs.getString(3));
            }
 
            rs.close();
            stmt.close();
            con.close();
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	
	
	// 連線DB2資料庫 GAMLDB
	public static Connection getDB2Connection() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		
//		String db2Driver = "com.ibm.db2.jcc.DB2Driver";
//		String url = "jdbc:db2://172.18.21.206:50000/sample";
//		String user = "Administrator";
//		String password = "9ol.)P:?";
//		Class.forName(db2Driver).newInstance();
		
		Class.forName(Profile.ETL_Profile.db2Driver).newInstance();
		String url = Profile.ETL_Profile.db2Url;
		String user = Profile.ETL_Profile.db2User;
		String password = Profile.ETL_Profile.db2Password;
		
		System.out.println(user + " 連線  " + url + " ...");
		Connection con = DriverManager.getConnection(url, user, password);
		con.setAutoCommit(true);
		System.out.println(user + " 連線成功!!");
		
		return con;
	}
	
	// 連線DB2資料庫 其他單位
	public static Connection getDB2Connection(String v_CENTRAL_NO) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {

		Class.forName(Profile.ETL_Profile.db2Driver).newInstance();
		String url =  Profile.ETL_Profile.getDB2Url(v_CENTRAL_NO.trim());
	
		String user = Profile.ETL_Profile.db2User;
		String password = Profile.ETL_Profile.db2Password;
		
		System.out.println(user + " 連線  " + url + " ...");
		Connection con = DriverManager.getConnection(url, user, password);
		con.setAutoCommit(true);
		System.out.println(user + " 連線成功!!");
		
		return con;
	}

	// 連線DB2資料庫GAML(xxx)
	public static Connection getDB2ConnGAML(String central_No) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		Class.forName(Profile.ETL_Profile.db2Driver).newInstance();
		String url = Profile.ETL_Profile.db2UrlGAMLpre + central_No + Profile.ETL_Profile.db2UrlGAMLafter;
		String user = Profile.ETL_Profile.GAML_db2User;
		String password = Profile.ETL_Profile.GAML_db2Password;
		
		System.out.println(user + " 連線  " + url + " ...");
		Connection con = DriverManager.getConnection(url, user, password);
		con.setAutoCommit(true);
		System.out.println(user + " 連線成功!!");
		
		return con;
	}
	
}
