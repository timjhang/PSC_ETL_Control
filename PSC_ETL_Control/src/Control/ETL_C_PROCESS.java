package Control;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Date;

import Bean.ETL_Bean_LogData;
import Bean.ETL_Bean_Response;
import DB.ConnectionHelper;
import Load.*;
import Profile.ETL_Profile;

public class ETL_C_PROCESS {
	// 執行ETL
	public static boolean executeETL(String[] etlServerInfo, String batch_No, String central_no, Date record_Date) {
		
		System.out.println("#### ETL_C_PROCESS  Start");
		
		// for test
		System.out.println("Server_No : " + etlServerInfo[0] + " , Server : " + etlServerInfo[1] + " , IP : " + etlServerInfo[2]);
		System.out.println("Batch_No = " + batch_No);
		System.out.println("Central_no = " + central_no);
		System.out.println("Record_Date = " + record_Date);
		
		String exeInfo = "Server_No : " + etlServerInfo[0] + " , Server : " + etlServerInfo[1] + " , IP : " + etlServerInfo[2] + "\n";
		exeInfo = exeInfo + "Batch_No = " + batch_No + "\n";
		exeInfo = exeInfo + "Central_no = " + central_no + "\n";
		exeInfo = exeInfo + "Record_Date = " + record_Date + "\n";
		
		// ETL Server下載特定中心資料
		String[] fileInfo = new String[3];
		String exc_record_date = "";
		String upload_no = "";
		boolean exeResult = true;
		
		// for test
//		exc_record_date = "20180227";
//		upload_no = "001";
		
		
		// **更新 Server 狀態使用中
		try {
			update_Server_Status(etlServerInfo[0], "U");
		} catch (Exception ex) {
			System.out.println("更新Server狀態\"使用中\"失敗");
			ex.printStackTrace();
			return false;
		}
		
		try {
			
			ETL_Bean_Response response = ETL_C_CallWS.call_ETL_Server_getUploadFileInfo(etlServerInfo[2], central_no);

			if(response.isSuccess()) {
				//取出物件轉型
				fileInfo = (String[]) response.getObj();
			}
			
			// 執行下載
			if (!response.isSuccess()) {
				System.out.println("#### ETL_C_PROCESS - executeETL - call_ETL_Server_getUploadFileInfo 發生錯誤！");
				return false;
			}
			exc_record_date = fileInfo[0];
			upload_no = fileInfo[1];
			
			System.out.println("#### ETL_C_PROCESS fileInfo[0]"+fileInfo[0]);
			System.out.println("#### ETL_C_PROCESSfileInfo[1]"+fileInfo[1]);
	
			// for test
//			Date testDate = new SimpleDateFormat("yyyyMMdd").parse("20180227");
//			record_Date = new SimpleDateFormat("yyyyMMdd").parse("20180227");

			
			// 更新報送單位狀態"使用中"
			updateCentralTime(central_no, record_Date, upload_no, "Start");

			
			// 寫入E Master Log
			if (!ETL_C_PROCESS.writeMasterLog(batch_No, central_no, record_Date, upload_no, "E", etlServerInfo[0])) {
	//			System.out.println("E Master Log已存在\n" + exeInfo);
				return false;
			};
			// 進行E系列程式
			if (!ETL_C_CallWS.call_ETL_Server_Efunction(etlServerInfo[2], "", batch_No, central_no, exc_record_date, upload_no)) {
				System.out.println("#### ETL_C_PROCESS - executeETL - call_ETL_Server_Efunction 發生錯誤！");
				return false;
			}
			// 更新 E Master Log
			ETL_C_PROCESS.updateMasterLog(batch_No, central_no, record_Date, upload_no, "E", "E", "Y", "");
			
			// 寫入T Master Log
			if (!ETL_C_PROCESS.writeMasterLog(batch_No, central_no, record_Date, upload_no, "T", etlServerInfo[0])) {
				System.out.println("T Master Log已存在\n" + exeInfo);
				return false;
			}
			// 進行T系列程式
			if (!ETL_C_CallWS.call_ETL_Server_Tfunction(etlServerInfo[2], "", batch_No, central_no, exc_record_date, upload_no)) {
				System.out.println("#### ETL_C_PROCESS - executeETL - call_ETL_Server_Tfunction 發生錯誤！");
				return false;
			}
			// 更新 T Master Log
			ETL_C_PROCESS.updateMasterLog(batch_No, central_no, record_Date, upload_no, "T", "E", "Y", "");
			
			
			// 寫入L Master Log
			if (!ETL_C_PROCESS.writeMasterLog(batch_No, central_no, record_Date, upload_no, "L", etlServerInfo[0])) {
				System.out.println("L Master Log已存在\n" + exeInfo);
				return false;
			}

			// 進行L系列程式  // TODO
			if (!exeLfunction(etlServerInfo[0], batch_No, central_no, exc_record_date, upload_no)) {
				System.out.println("#### ETL_C_PROCESS - executeETL - exeLfunction 發生錯誤！");
				return false;
			}

			// 更新 L Master Log
			ETL_C_PROCESS.updateMasterLog(batch_No, central_no, record_Date, upload_no, "L", "E", "Y", "");

		
		} catch (Exception ex) {
			ex.printStackTrace();
			exeResult = false;
		} finally {
			// 更新報送單位狀態"執行完畢"
			try {
				updateCentralTime(central_no, record_Date, upload_no, "End");
			} catch (Exception ex) {
				System.out.println("更新Central:" + central_no + " - 狀態:\"執行完畢\"失敗");
				ex.printStackTrace();
				exeResult = false;
			}
		}
		
		// **更新 Server 狀態可使用
		try {
			update_Server_Status(etlServerInfo[0], "Y");
		} catch (Exception ex) {
			System.out.println("更新Server狀態\"可使用\"失敗");
			ex.printStackTrace();
			exeResult = false;
		}
		
		System.out.println("#### ETL_C_PROCESS  End");
		
		return exeResult;
		
	}
	
	// 寫入Master Log
	private static boolean writeMasterLog(String batch_No, String central_No, Date record_Date,
			String upload_No, String step_Type, String server_No) {
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.write_Master_Log(?,?,?,?,?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setString(2, batch_No);
			cstmt.setString(3, central_No);
			cstmt.setDate(4, new java.sql.Date(record_Date.getTime()));
			cstmt.setString(5, upload_No);
			cstmt.setString(6, step_Type);
			cstmt.setString(7, server_No);
			cstmt.registerOutParameter(8, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(8);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
//			            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            
	            return false;
			}
			
			return true;
	        
		} catch (Exception ex) {
			ex.printStackTrace();
			return false;
		}
		
	}
	
	// 更新Master Log
	private static boolean updateMasterLog(String batch_No, String central_No, Date record_Date, String upload_No, String step_Type,
			String exe_Status, String exe_Result, String exe_Result_Description) {
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.update_Master_Log(?,?,?,?,?,?,?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setString(2, batch_No);
			cstmt.setString(3, central_No);
			cstmt.setDate(4, new java.sql.Date(record_Date.getTime()));
			cstmt.setString(5, upload_No);
			cstmt.setString(6, step_Type);
			cstmt.setString(7, exe_Status);
			cstmt.setString(8, exe_Result);
			cstmt.setString(9, exe_Result_Description);
			cstmt.registerOutParameter(10, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(8);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
//			            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            
	            return false;
			}
			
			return true;
	        
		} catch (Exception ex) {
			ex.printStackTrace();
			return false;
		}
		
	}
	
	// 更新 Server 狀態使用中
	private static void update_Server_Status(String server_No, String usable_Status) throws Exception {
		
		String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.update_Server_Status(?,?,?,?)}";
		
		Connection con = ConnectionHelper.getDB2Connection();
		CallableStatement cstmt = con.prepareCall(sql);
		
		cstmt.registerOutParameter(1, Types.INTEGER);
		cstmt.setString(2, server_No);
		cstmt.setString(3, usable_Status);
		cstmt.registerOutParameter(4, Types.VARCHAR);
		
		cstmt.execute();
		
		int returnCode = cstmt.getInt(1);
		
		// 有錯誤釋出錯誤訊息   不往下繼續進行
		if (returnCode != 0) {
			String errorMessage = cstmt.getString(4);
			System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
		}
			
	}
	
	// 更新報送單位狀態
	private static void updateCentralTime(String central_No, Date record_Date, String upload_No, String status) throws Exception {
		
		//for test
		System.out.println("##########update_Central_Time Start");
		System.out.println("central_No:"+central_No);
		System.out.println("record_Date:"+record_Date);
		System.out.println("upload_No:"+upload_No);
		System.out.println("status:"+status);
		System.out.println("##########update_Central_Time End");
		
		String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.update_Central_Time(?,?,?,?,?,?)}";
		
		Connection con = ConnectionHelper.getDB2Connection();
		CallableStatement cstmt = con.prepareCall(sql);
		
		cstmt.registerOutParameter(1, Types.INTEGER);
		cstmt.setString(2, central_No);
		cstmt.setDate(3, new java.sql.Date(record_Date.getTime()));
		cstmt.setString(4, upload_No);
		cstmt.setString(5, status);
		cstmt.registerOutParameter(6, Types.VARCHAR);
		
		cstmt.execute();
		
		int returnCode = cstmt.getInt(1);
		
		// 有錯誤釋出錯誤訊息   不往下繼續進行
		if (returnCode != 0) {
			String errorMessage = cstmt.getString(6);
//			System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
		}
		
	}
	
	// 執行L系列程式  // TODO
	public static boolean exeLfunction(String server_No, String batch_No, String exc_central_no, String record_DateStr,
			String upload_No) {
		System.out.println("call_ETL_Server_Lfunction : 開始執行");
		try {

			ETL_Bean_LogData logData = new ETL_Bean_LogData();
			logData.setBATCH_NO(batch_No);
			logData.setCENTRAL_NO(exc_central_no);
			logData.setFILE_TYPE(null);
			Date exc_record_date = new SimpleDateFormat("yyyyMMdd").parse(record_DateStr);
			logData.setRECORD_DATE(exc_record_date);
			logData.setUPLOAD_NO(upload_No);

			String fedServer = "";
			// 設定fedName
			if ("ETL_S1".equals(server_No)) {
				fedServer = "ETLDB001";
			} else if ("ETL_S2".equals(server_No)) {
				fedServer = "ETLDB002";
			}

			// 執行用Table (正常 rerun, 重跑rerun)
			String runTable = "temp";
			
			
			System.out.println("batch_No:" + batch_No);
			System.out.println("exc_central_no:" + exc_central_no);
			System.out.println("exc_record_date:" + exc_record_date);
			System.out.println("upload_No:" + upload_No);
			System.out.println("server_No:" + server_No);
			System.out.println("runTable:" + runTable);

			// 執行20支L系列程式
			logData.setPROGRAM_NO("ETL_L_PARTY_PHONE");
			new ETL_L_PARTY_PHONE().trans_to_PARTY_PHONE_LOAD(logData, fedServer, runTable);
			
			logData.setPROGRAM_NO("ETL_L_CALENDAR");
			new ETL_L_CALENDAR().trans_to_CALENDAR_LOAD(logData, fedServer, runTable);
			
			logData.setPROGRAM_NO("ETL_L_COLLATERAL");
			new ETL_L_COLLATERAL().trans_to_COLLATERAL_LOAD(logData, fedServer, runTable);
			
			logData.setPROGRAM_NO("ETL_L_FX_RATE");
			new ETL_L_FX_RATE().trans_to_FX_RATE_LOAD(logData, fedServer, runTable);
			
			logData.setPROGRAM_NO("ETL_L_LOAN_DETAIL");
			new ETL_L_LOAN_DETAIL().trans_to_LOAN_DETAIL_LOAD(logData, fedServer, runTable);
			
			logData.setPROGRAM_NO("ETL_L_LOAN_GUARANTOR");
			new ETL_L_LOAN_GUARANTOR().trans_to_LOAN_GUARANTOR_LOAD(logData, fedServer, runTable);
			
			logData.setPROGRAM_NO("ETL_L_LOAN_MASTER");
			new ETL_L_LOAN_MASTER().trans_to_LOAN_MASTER_LOAD(logData, fedServer, runTable);
			
			logData.setPROGRAM_NO("ETL_L_LOAN");
			new ETL_L_LOAN().trans_to_LOAN_LOAD(logData, fedServer, runTable);
			
			logData.setPROGRAM_NO("ETL_L_SERVICE");
			new ETL_L_SERVICE().trans_to_SERVICE_LOAD(logData, fedServer, runTable);
			
			logData.setPROGRAM_NO("ETL_L_TRANSACTION");
			new ETL_L_TRANSACTION().trans_to_TRANSACTION_LOAD(logData, fedServer, runTable);
			
			logData.setPROGRAM_NO("ETL_L_TRANSFER");
			new ETL_L_TRANSFER().trans_to_TRANSFER_LOAD(logData, fedServer, runTable);
			
			logData.setPROGRAM_NO("ETL_L_ACCOUNT_PROPERTY");
			new ETL_L_ACCOUNT_PROPERTY().trans_to_ACCOUNT_PROPERTY_LOAD(logData, fedServer, runTable);
			
			logData.setPROGRAM_NO("ETL_L_ACCOUNT");
			new ETL_L_ACCOUNT().trans_to_ACCOUNT_LOAD(logData, fedServer, runTable);
			
			logData.setPROGRAM_NO("ETL_L_BALANCE");
			new ETL_L_BALANCE().trans_to_BALANCE_LOAD(logData, fedServer, runTable);
			
			logData.setPROGRAM_NO("ETL_L_PARTY_ACCOUNT_REL");
			new ETL_L_PARTY_ACCOUNT_REL().trans_to_PARTY_ACCOUNT_REL_LOAD(logData, fedServer, runTable);
			
			logData.setPROGRAM_NO("ETL_L_PARTY_ADDRESS");
			new ETL_L_PARTY_ADDRESS().trans_to_PARTY_ADDRESS_LOAD(logData, fedServer, runTable);
			
			logData.setPROGRAM_NO("ETL_L_PARTY_EMAIL");
			new ETL_L_PARTY_EMAIL().trans_to_PARTY_EMAIL_LOAD(logData, fedServer, runTable);
			
			logData.setPROGRAM_NO("ETL_L_PARTY_NATINOALITY");
			new ETL_L_PARTY_NATINOALITY().trans_to_PARTY_NATINOALITY_LOAD(logData, fedServer, runTable);
			
			logData.setPROGRAM_NO("ETL_L_PARTY_PARTY_REL");
			new ETL_L_PARTY_PARTY_REL().trans_to_PARTY_PARTY_REL_LOAD(logData, fedServer, runTable);
			
			logData.setPROGRAM_NO("ETL_L_PARTY");
			new ETL_L_PARTY().trans_to_PARTY_LOAD(logData, fedServer, runTable);

		

		} catch (Exception ex) {
			System.out.println("call_ETL_Server_Lfunction : 發生錯誤");
			ex.printStackTrace();
			return false;
		}
		System.out.println("call_ETL_Server_Lfunction : 結束");
		return true;
	}
	
	public static void main(String[] argv) {
		try {
			String[] serverInfo = new String[3];
			serverInfo[0] = "ETL_S1";
			serverInfo[1] = "test2";
			serverInfo[2] = "127.0.0.1:8083";
			Date date = new Date();
			executeETL(serverInfo, "tim18226", "600", date);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}