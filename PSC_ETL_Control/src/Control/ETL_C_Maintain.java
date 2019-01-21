package Control;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import DB.ConnectionHelper;
import DB.ETL_P_Log;
import Profile.ETL_Profile;

public class ETL_C_Maintain {
	
	public static void main(String[] argvs) {
		
		try {
			
			System.out.println("測試 Start");
			
			execute();
			
			System.out.println("測試 End");
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
	}
	
	public static void execute() throws InterruptedException {
		
		List<String> centralList = new ArrayList<String>();
		centralList.add("018");
		centralList.add("600");
//		centralList.add("605");
		centralList.add("910");
		centralList.add("928");
		centralList.add("951");
		centralList.add("952");
		
		// 讀取是否啟動維護參數
		
		// 若Rerun執行, 不進行資料表維護
    	if (ETL_C_Master.isRerunExecute()) {
    		System.out.println("####ETL_C_Maintain - Rerun 作業進行, 不進行資料表維護作業。  ");
    		ETL_P_Log.write_Runtime_Log("ETL_C_Maintain", "####ETL_C_Maintain - Rerun 作業進行, 不進行資料表維護作業。");
    		return;
    	}
    	
    	// 若ETL計畫執行中, 不進行資料表維護
    	if (ETL_C_Migration.isETLplanExecute(new String[1], new String[1])) {
    		System.out.println("####ETL_C_Maintain - ETL 計畫執行中, 不進行資料表維護作業。  ");
    		ETL_P_Log.write_Runtime_Log("ETL_C_Maintain", "####ETL_C_Maintain - ETL 計畫執行中, 不進行資料表維護作業。");
    		return;
    	}
    	
		System.out.println("#### ETL_C_Maintain Start  " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
		System.out.println("開始執行每日ETL常用資料表維護");
		
		System.out.println("維護DB GAMLDB Start  " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		maintain_GAMLDB_Tables();
		System.out.println("維護DB GAMLDB End  " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
		for (int i = 0; i < centralList.size(); i++) {
			System.out.println("維護DB GAML" + centralList.get(i) + " Start  " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
			maintain_GAML_Tables(centralList.get(i));
			System.out.println("維護DB GAML" + centralList.get(i) + " End  " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		}
		
		System.out.println("維護DB ETLDB001  Start  " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		maintain_ETLDB_Tables("001");
		System.out.println("維護DB ETLDB001  End  " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
//		System.out.println("維護DB ETLDB002  Start  " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
//		maintain_ETLDB_Tables("002");
//		System.out.println("維護DB ETLDB002  End  " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
		System.out.println("#### ETL_C_Maintain End  " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
	}
	
	// 維護GAMLDB tables
	private static void maintain_GAMLDB_Tables() {
		
		System.out.println("#######maintain_GAMLDB_Tables - Start");
		
		Connection con = null;
		CallableStatement cstmt = null;
		
		try {
		
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Maintain_Control.maintain_Tables(?,?)}";
			
			con = ConnectionHelper.getDB2Connection();
			cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.registerOutParameter(2, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(2);
			    System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}
		
		} catch (Exception ex) {
			ex.printStackTrace();
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			ex.printStackTrace(pw);
			System.out.println("ExceptionMassage:" + sw.toString());
		} finally {
			try {
				if (con != null) {
					con.close();
				}
				if (cstmt != null) {
					cstmt.close();
				}
			} catch (Exception ex) {
//				ex.printStackTrace();
			}
		}
			
		System.out.println("#######maintain_GAMLDB_Tables - End");
		
	}
	
	// 維護GAML018, 600, ... tables
	private static void maintain_GAML_Tables(String DB_no) {
		
		System.out.println("#######maintain_GAML_Tables " + DB_no + " - Start");
		
		Connection con = null;
		CallableStatement cstmt = null;
		
		try {
		
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Maintain_Central.maintain_Tables(?,?)}";
			
			con = ConnectionHelper.getDB2ConnGAML(DB_no);
			cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.registerOutParameter(2, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(2);
			    System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}
		
		} catch (Exception ex) {
			ex.printStackTrace();
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			ex.printStackTrace(pw);
			System.out.println("ExceptionMassage:" + sw.toString());
		} finally {
			try {
				if (con != null) {
					con.close();
				}
				if (cstmt != null) {
					cstmt.close();
				}
			} catch (Exception ex) {
//				ex.printStackTrace();
			}
		}
			
		System.out.println("#######maintain_GAML_Tables " + DB_no + " - End");
		
	}
	
	// 維護ETLDB001, ETLDB002 tables
	private static void maintain_ETLDB_Tables(String ETLDB_no) {
		
		System.out.println("#######maintain_ETLDB_Tables " + ETLDB_no + " - Start");
		
		Connection con = null;
		CallableStatement cstmt = null;
		
		try {
		
			String sql = "{call " + ETL_Profile.db2ETLTableSchema + ".ETL_SERVER_SERVICE.maintain_Tables(?,?)}";
			
			con = ConnectionHelper.getETLDB2Connection(ETLDB_no);
			cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.registerOutParameter(2, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(2);
			    System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}
		
		} catch (Exception ex) {
			ex.printStackTrace();
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			ex.printStackTrace(pw);
			System.out.println("ExceptionMassage:" + sw.toString());
		} finally {
			try {
				if (con != null) {
					con.close();
				}
				if (cstmt != null) {
					cstmt.close();
				}
			} catch (Exception ex) {
//				ex.printStackTrace();
			}
		}
			
		System.out.println("#######maintain_ETLDB_Tables " + ETLDB_no + " - End");
		
	}

}
