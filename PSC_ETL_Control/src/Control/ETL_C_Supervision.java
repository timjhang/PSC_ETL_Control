package Control;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.ibm.db2.jcc.DB2Types;

import DB.ConnectionHelper;
import DB.ETL_P_Log;
import Profile.ETL_Profile;
import Tool.ETL_Tool_Mail;

public class ETL_C_Supervision {

	String ETL_Mail_Type = "";
	
	// ETL監控寫入mail批次
	public void execute() {
		
		System.out.println("####ETL_C_Supervision 寄發通知信  Start " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
		Date record_date;
		try {
			record_date = ETL_C_Master.getBeforeRecordDate(new Date());
		} catch (Exception ex) {
			ex.printStackTrace();
        	System.out.println("####ETL_C_Supervision 無法取得資料日期，無法繼續進行！");
        	ETL_P_Log.write_Runtime_Log("ETL_C_Supervision", "####ETL_C_Supervision 無法取得資料日期，無法繼續進行！");
        	
        	// 寫入執行成功信件
			String mailContent = 
					"監控程式  無法取得資料日期";
			ETL_Tool_Mail.writeAML_Mail("SYSAdmin", null, null, "AML_ETL 系統通知", mailContent);
        	return;
		}
		
		// 上班日進行提醒
		if (isBusinessDay(new Date())) {
		
			if ("1".equals(ETL_Mail_Type)) {
				System.out.println("早晨監控");
				
				// 未執行ETL
				List<String> nonETLCentralList = getNon_ETL_Centrals(record_date);
				for (int i = 0; i < nonETLCentralList.size(); i++) {
					// 寫入執行成功信件
					String mailContent = 
							"單位：" + nonETLCentralList.get(i) + "  資料日期：" + new SimpleDateFormat("yyyyMMdd").format(record_date) +
							"  尚未執行AML_ETL，請檢查檔案是否上傳？";
					ETL_Tool_Mail.writeAML_Mail(nonETLCentralList.get(i), "SYSAdmin", null, "AML_ETL 系統通知", mailContent);
				}
				
				// 執行中ETL
				List<String> exeETLCentralList = getExe_ETL_Centrals(record_date);
				for (int i = 0; i < exeETLCentralList.size(); i++) {
					// 寫入執行成功信件
					String mailContent = 
							"單位：" + exeETLCentralList.get(i) + "  資料日期：" + new SimpleDateFormat("yyyyMMdd").format(record_date) +
							"  AML_ETL現在執行中。";
					ETL_Tool_Mail.writeAML_Mail(nonETLCentralList.get(i), "SYSAdmin", null, "AML_ETL 系統通知", mailContent);
				}
				
				// ETL執行錯誤
				List<String> errETLCentralList = getErr_ETL_Centrals(record_date);
				for (int i = 0; i < errETLCentralList.size(); i++) {
					// 寫入執行成功信件
					String mailContent = 
							"單位：" + exeETLCentralList.get(i) + "  資料日期：" + new SimpleDateFormat("yyyyMMdd").format(record_date) +
							"  AML_ETL 執行出現錯誤。\n請洽IT人員查詢原因, 並重送異動資料進行重跑。";
					ETL_Tool_Mail.writeAML_Mail(nonETLCentralList.get(i), "SYSAdmin", null, "AML_ETL 系統通知", mailContent);
				}
				
			} else if ("2".equals(ETL_Mail_Type)) {
				System.out.println("中午監控");
				
				// 未執行ETL
				List<String> nonETLCentralList = getNon_ETL_Centrals(record_date);
				for (int i = 0; i < nonETLCentralList.size(); i++) {
					// 寫入執行成功信件
					String mailContent = 
							"單位：" + nonETLCentralList.get(i) + "  資料日期：" + new SimpleDateFormat("yyyyMMdd").format(record_date) +
							"  仍未執行AML_ETL，請檢查檔案是否上傳？\n超過AML_ETL正常執行區段，若補送檔案，須請系統管理員協助重跑。";
					ETL_Tool_Mail.writeAML_Mail(nonETLCentralList.get(i), "SYSAdmin", null, "AML_ETL 系統通知", mailContent);
				}
				
				// 執行中ETL
				List<String> exeETLCentralList = getExe_ETL_Centrals(record_date);
				for (int i = 0; i < exeETLCentralList.size(); i++) {
					// 寫入執行成功信件
					String mailContent = 
							"單位：" + exeETLCentralList.get(i) + "  資料日期：" + new SimpleDateFormat("yyyyMMdd").format(record_date) +
							"  AML_ETL現在執行中。";
					ETL_Tool_Mail.writeAML_Mail(nonETLCentralList.get(i), "SYSAdmin", null, "AML_ETL 系統通知", mailContent);
				}
				
				// ETL執行錯誤
				List<String> errETLCentralList = getErr_ETL_Centrals(record_date);
				for (int i = 0; i < errETLCentralList.size(); i++) {
					// 寫入執行成功信件
					String mailContent = 
							"單位：" + exeETLCentralList.get(i) + "  資料日期：" + new SimpleDateFormat("yyyyMMdd").format(record_date) +
							"  AML_ETL 執行出現錯誤。\n請洽IT人員查詢原因, 並重送異動資料進行重跑。";
					ETL_Tool_Mail.writeAML_Mail(nonETLCentralList.get(i), "SYSAdmin", null, "AML_ETL 系統通知", mailContent);
				}
				
			}
		
		} else {
			System.out.println("非上班日不進行監控提醒！！");
		}
		
		System.out.println("####ETL_C_Supervision 寄發通知信  End " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
	}
	
	public String getETL_Mail_Type() {
		return ETL_Mail_Type;
	}

	public void setETL_Mail_Type(String eTL_Mail_Type) {
		ETL_Mail_Type = eTL_Mail_Type;
	}
	
	// 取得單位未執行ETL單位
	private static List<String> getNon_ETL_Centrals(Date record_date) {
		CallableStatement cstmt = null;
		java.sql.ResultSet rs = null;
		Connection con = null;
		
		List<String> resultList = new ArrayList<String>();
		
		try {

			String sql = "{call " + ETL_Profile.db2TableSchema + ".AML_EMAIL.getNon_ETL_Centrals(?,?,?,?)}";
//				String sql = "{call " + ETL_Profile.GAML_db2TableSchema + ".AML_EMAIL.getNon_ETL_Centrals(?,?,?,?)}"; // for test

			con = ConnectionHelper.getDB2Connection();
//				con = ConnectionHelper.getDB2ConnGAML("DB"); // for test
			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(record_date.getTime()));
			cstmt.registerOutParameter(3, DB2Types.CURSOR);
			cstmt.registerOutParameter(4, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			// 有錯誤釋出錯誤訊息 不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(4);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
				return resultList;
			}

			// (java.sql.ResultSet)
			rs = (java.sql.ResultSet) cstmt.getObject(3);

			if (rs == null) {
				return resultList;
			}

			while (rs.next()) {
				String central_no = rs.getString(1);
				if (central_no != null && !"".equals(central_no.trim())) {
					resultList.add(central_no);
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				// 資源後開,先關
				if (cstmt != null) {
					cstmt.close();
				}
				if (con != null) {
					con.close();
				}

				if (rs != null) {
					rs.close();
				}

			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return resultList;
	}
	
	// 取得單位執行中ETL單位
	private static List<String> getExe_ETL_Centrals(Date record_date) {
		CallableStatement cstmt = null;
		java.sql.ResultSet rs = null;
		Connection con = null;
		
		List<String> resultList = new ArrayList<String>();
		
		try {

			String sql = "{call " + ETL_Profile.db2TableSchema + ".AML_EMAIL.getExe_ETL_Centrals(?,?,?,?)}";
//					String sql = "{call " + ETL_Profile.GAML_db2TableSchema + ".AML_EMAIL.getExe_ETL_Centrals(?,?,?,?)}"; // for test

			con = ConnectionHelper.getDB2Connection();
//					con = ConnectionHelper.getDB2ConnGAML("DB"); // for test
			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(record_date.getTime()));
			cstmt.registerOutParameter(3, DB2Types.CURSOR);
			cstmt.registerOutParameter(4, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			// 有錯誤釋出錯誤訊息 不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(4);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
				return resultList;
			}

			// (java.sql.ResultSet)
			rs = (java.sql.ResultSet) cstmt.getObject(3);

			if (rs == null) {
				return resultList;
			}

			while (rs.next()) {
				String central_no = rs.getString(1);
				if (central_no != null && !"".equals(central_no.trim())) {
					resultList.add(central_no);
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				// 資源後開,先關
				if (cstmt != null) {
					cstmt.close();
				}
				if (con != null) {
					con.close();
				}

				if (rs != null) {
					rs.close();
				}

			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return resultList;
	}
	
	// 取得單位執行中ETL單位
	private static List<String> getErr_ETL_Centrals(Date record_date) {
		CallableStatement cstmt = null;
		java.sql.ResultSet rs = null;
		Connection con = null;
		
		List<String> resultList = new ArrayList<String>();
		
		try {

			String sql = "{call " + ETL_Profile.db2TableSchema + ".AML_EMAIL.getErr_ETL_Centrals(?,?,?,?)}";
//						String sql = "{call " + ETL_Profile.GAML_db2TableSchema + ".AML_EMAIL.getErr_ETL_Centrals(?,?,?,?)}"; // for test

			con = ConnectionHelper.getDB2Connection();
//						con = ConnectionHelper.getDB2ConnGAML("DB"); // for test
			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(record_date.getTime()));
			cstmt.registerOutParameter(3, DB2Types.CURSOR);
			cstmt.registerOutParameter(4, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			// 有錯誤釋出錯誤訊息 不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(4);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
				return resultList;
			}

			// (java.sql.ResultSet)
			rs = (java.sql.ResultSet) cstmt.getObject(3);

			if (rs == null) {
				return resultList;
			}

			while (rs.next()) {
				String central_no = rs.getString(1);
				if (central_no != null && !"".equals(central_no.trim())) {
					resultList.add(central_no);
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				// 資源後開,先關
				if (cstmt != null) {
					cstmt.close();
				}
				if (con != null) {
					con.close();
				}

				if (rs != null) {
					rs.close();
				}

			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return resultList;
	}
	
	// 確認是否為營業日
	public static boolean isBusinessDay(Date recordDate) {
		CallableStatement cstmt = null;
		Connection con = null;
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.isBusinessDay(?,?,?,?)}";
			
			con = ConnectionHelper.getDB2Connection();
			cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(recordDate.getTime()));
			cstmt.registerOutParameter(3, Types.VARCHAR);
			cstmt.registerOutParameter(4, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(4);
		        System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
		        return false;
			}
			
			String result = cstmt.getString(3);
			
			if ("Y".equals(result)) {
				return true;
			} else {
				return false;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			return false;
		} finally {
			try {
				// 資源後開,先關
				if (cstmt != null) {
					cstmt.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] argvs) {
		System.out.println("測試開始");

		try {
//			List<String> list = getNon_ETL_Centrals(new SimpleDateFormat("yyyyMMdd").parse("20180627"));
//			List<String> list = getExe_ETL_Centrals(new SimpleDateFormat("yyyyMMdd").parse("20180627"));
			List<String> list = getErr_ETL_Centrals(new SimpleDateFormat("yyyyMMdd").parse("20180627"));
			for (int i = 0; i < list.size(); i++) {
				System.out.println(list.get(i));
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		System.out.println("測試結束");
	}
	
}
