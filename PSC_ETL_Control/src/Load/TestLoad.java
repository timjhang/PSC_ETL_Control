package Load;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Struct;
import java.sql.Types;

import DB.ConnectionHelper;
import Profile.ETL_Profile;
import Tool.ETL_Tool_CastObjUtil;

public class TestLoad {
	
//	public static void main(String[] argv) throws IOException, InterruptedException {
//		
//		System.out.println("#### L - 1");
//		
////		try {
////			String sql = "CREATE TABLE \"ADMINISTRATOR\".\"TIMTEST\"(\"CENTRAL_NAME\" VARCHAR(100) , \"RECORD_DATE\" DATE)";
////			
////			Connection con = ConnectionHelper.getDB2Connection();
////			CallableStatement cstmt = con.prepareCall(sql);
////			
////			cstmt.execute();
////			
////		} catch (Exception ex) {
////			ex.printStackTrace();
////		}
//		
//		System.out.println("#### L - 2");
//		
//		try {
//			String sql = "declare c1 cursor for select * from \"ADMINISTRATOR\".\"TIMTEST_P\" where RECORD_DATE = '1/5/2018'";
//			
//			Connection con = ConnectionHelper.getDB2Connection();
//			CallableStatement cstmt = con.prepareCall(sql);
//			
//			cstmt.execute();
//			
//		} catch (Exception ex) {
//			ex.printStackTrace();
//		}
//		
//		System.out.println("#### L - 3");
//		
//		try {
//			String sql = "load from c1 of cursor insert into \"ADMINISTRATOR\".\"TIMTEST\" copy yes to C:/test2/db2";
//			
//			Connection con = ConnectionHelper.getDB2Connection();
//			CallableStatement cstmt = con.prepareCall(sql);
//			
//			cstmt.execute();
//			
//		} catch (Exception ex) {
//			ex.printStackTrace();
//		}
//		
//		System.out.println("#### L - 4");
//		System.out.println("#### L - 5");
//		System.out.println("#### L - 6");
//		System.out.println("#### L - 7");
//		
//	}
	
	public static void main(String[] argv) throws IOException, InterruptedException {
		
		System.out.println("小試一下");
		Runtime runtime = Runtime.getRuntime();
//		Process proc = runtime.exec("DB2cmd /c db2 force application all");
		Process proc = runtime.exec("DB2cmd /c db2 force application all");
		
//		String createStr = "db2cmd db2 connect to sample user administrator using \"1qaz@WSX\" " + "\n";
//		createStr = createStr + "db2cmd db2 CREATE TABLE \"ADMINISTRATOR\".\"TIMTEST\"(\"CENTRAL_NAME\" VARCHAR(100) , \"RECORD_DATE\" DATE)";
//		proc = runtime.exec(createStr);
		
//		proc = runtime.exec("db2cmd /c db2 connect to sample user administrator using \"1qaz@WSX\" ");
//		Thread.currentThread().sleep(3000);
		proc = runtime.exec("db2cmd /c db2 CREATE TABLE \"ADMINISTRATOR\".\"TIMTEST\"(\"CENTRAL_NAME\" VARCHAR(100) , \"RECORD_DATE\" DATE)");
		
		proc.destroy();
		runtime.exit(0);
	}
	
	public void load_TimTest() {
		
	}

}
