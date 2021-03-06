package Control;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ibm.db2.jcc.DB2Types;

import Bean.ETL_Bean_LogData;
import Bean.ETL_Bean_Response;
import DB.ConnectionHelper;
import DB.ETL_P_Log;
import Load.*;
import Profile.ETL_Profile;
import Tool.ETL_Tool_StringX;

public class ETL_C_PROCESS {
	
	// 是否為正式參數
	private static boolean isFormal = true;
	
	// 執行ETL
	public static boolean executeETL(String[] etlServerInfo, String batch_No, String central_no, String[] ptr_upload_no, Date record_Date, Date before_record_date) {
		
		System.out.println("#### ETL_C_PROCESS - executeETL  Start");
		
		// for test
		System.out.println("Server_No : " + etlServerInfo[0] + " , Server : " + etlServerInfo[1] + " , IP : " + etlServerInfo[2]);
		System.out.println("Batch_No = " + batch_No);
		System.out.println("Central_no = " + central_no);
		System.out.println("Record_Date = " + record_Date);
		System.out.println("Before_record_date = " + before_record_date);
		
		String exeInfo = "Server_No : " + etlServerInfo[0] + " , Server : " + etlServerInfo[1] + " , IP : " + etlServerInfo[2] + "\n";
		exeInfo = exeInfo + "Batch_No = " + batch_No + "\n";
		exeInfo = exeInfo + "Central_no = " + central_no + "\n";
		exeInfo = exeInfo + "Record_Date = " + record_Date + "\n";
		exeInfo = exeInfo + "Before_record_date = " + before_record_date + "\n";
		
		String server_no = etlServerInfo[0];
		
		// ETL Server下載特定中心資料
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String[] fileInfo = new String[3];
		String exc_record_dateStr = sdf.format(record_Date);
		String upload_no = "";
		String before_record_dateStr = sdf.format(before_record_date);
		boolean exeResult = true;
		
		// **更新 Server 狀態使用中
		try {
			update_Server_Status(etlServerInfo[0], "U");
		} catch (Exception ex) {
			System.out.println("更新Server狀態\"使用中\"失敗 " + server_no);
			ETL_P_Log.write_Runtime_Log("executeETL", "更新Server狀態\"使用中\"失敗 " + server_no);
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("executeETL", ex.getMessage());
			return false;
		}
		
		try {
			// 更新 新5代製作紀錄檔
			updateNewGenerationETLStatus(record_Date, central_no, "Start", "");
			
			// 呼叫ETL Server進行initial作業
			if (!ETL_C_CallWS.call_ETL_Server_initETLserver(etlServerInfo[2])) {
//				System.out.println("#### ETL_C_PROCESS - executeETL - call_ETL_Server_initETLserver 發生錯誤！ " + server_no);
//				return false;
				throw new Exception("#### ETL_C_PROCESS - executeETL - call_ETL_Server_initETLserver 發生錯誤！ " + server_no);
			}
			
			// 下載中心檔案
			ETL_Bean_Response response = ETL_C_CallWS.call_ETL_Server_getUploadFileInfo(etlServerInfo[2], central_no, "", null);

			if (response.isSuccess()) {
				//取出物件轉型, <資料日期|上傳批號 |zip檔名>
				fileInfo = (String[]) response.getObj();
			}
			
			// 執行下載
			if (!response.isSuccess()) {
				throw new Exception("#### ETL_C_PROCESS - executeETL - call_ETL_Server_getUploadFileInfo 發生錯誤！ " + server_no);
			}

			upload_no = fileInfo[1];
			ptr_upload_no[0] = upload_no;
			
			System.out.println("#### ETL_C_PROCESS fileInfo[0]" + fileInfo[0] + " " + server_no);
			System.out.println("#### ETL_C_PROCESS fileInfo[1]" + fileInfo[1] + " " + server_no);
	
			// 更新報送單位狀態"使用中"
			updateCentralTime(central_no, record_Date, upload_no, "Start");
			
			// 清除 ERROR_LOG_DOWNLOAD
			boolean clearErrorDowbload = true;
			clearErrorDowbload = ETL_C_CallWS.call_ETL_Server_clear_ERROR_LOG_DOWNLOAD(etlServerInfo[2]);
			
			// 寫入E Master Log
			if (!ETL_C_PROCESS.writeMasterLog(batch_No, central_no, record_Date, upload_no, "E", etlServerInfo[0])) {
				throw new Exception("E Master Log已存在\n" + exeInfo);
			};
			// 進行E系列程式
			if (!ETL_C_CallWS.call_ETL_Server_Efunction(etlServerInfo[2], "", batch_No, central_no, exc_record_dateStr, upload_no)) {
				throw new Exception("#### ETL_C_PROCESS - executeETL - call_ETL_Server_Efunction 發生錯誤！ " + server_no);
			}
			// 更新 E Master Log
			ETL_C_PROCESS.updateMasterLog(batch_No, central_no, record_Date, upload_no, "E", "E", "Y", "");
			
			// Error Log 寫入 ERROR_LOG_DOWNLOAD
			if (clearErrorDowbload) {
				ETL_C_CallWS.call_ETL_Server_write_ERROR_LOG_DOWNLOAD(etlServerInfo[2], exc_record_dateStr, central_no, upload_no);
			}
			
			// 寫入T Master Log
			if (!ETL_C_PROCESS.writeMasterLog(batch_No, central_no, record_Date, upload_no, "T", etlServerInfo[0])) {
				throw new Exception("T Master Log已存在\n" + exeInfo);
			}
			// 進行T系列程式
			if (!ETL_C_CallWS.call_ETL_Server_Tfunction(etlServerInfo[2], "", batch_No, central_no, exc_record_dateStr, upload_no, before_record_dateStr)) {
				throw new Exception("#### ETL_C_PROCESS - executeETL - call_ETL_Server_Tfunction 發生錯誤！ " + server_no);
			}
			// 更新 T Master Log
			ETL_C_PROCESS.updateMasterLog(batch_No, central_no, record_Date, upload_no, "T", "E", "Y", "");
			
			
			// 寫入L Master Log
			if (!ETL_C_PROCESS.writeMasterLog(batch_No, central_no, record_Date, upload_no, "L", etlServerInfo[0])) {
				throw new Exception("L Master Log已存在\n" + exeInfo);
			}
			// 進行L系列程式
			if (!exeLfunction(etlServerInfo[0], batch_No, central_no, exc_record_dateStr, upload_no, before_record_dateStr, "TEMP")) {
				throw new Exception("#### ETL_C_PROCESS - executeETL - exeLfunction 發生錯誤！ " + server_no);
			}
			// 更新 L Master Log
			ETL_C_PROCESS.updateMasterLog(batch_No, central_no, record_Date, upload_no, "L", "E", "Y", "");

			
			// 執行暫存Table(load_temp)併入五代
			addNew5G(batch_No, record_Date, central_no, upload_no, "TEMP");
			
			
			// 更新 新5代製作紀錄檔
			updateNewGenerationETLStatus(record_Date, central_no, "End", "");
			
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("executeETL", ex.getMessage());
			updateNewGenerationETLStatus(record_Date, central_no, "Error", ex.getMessage());
			exeResult = false;
		} finally {
			// 更新報送單位狀態"執行完畢"
			try {
				updateCentralTime(central_no, record_Date, upload_no, "End");
			} catch (Exception ex) {
				System.out.println("更新Central:" + central_no + " - 狀態:\"執行完畢\"失敗 " + server_no);
				ETL_P_Log.write_Runtime_Log("executeETL", "更新Central:" + central_no + " - 狀態:\"執行完畢\"失敗 " + server_no);
				ex.printStackTrace();
				exeResult = false;
			}
		}
		
		// **更新 Server 狀態可使用
		try {
			update_Server_Status(etlServerInfo[0], "Y");
		} catch (Exception ex) {
			System.out.println("更新Server狀態\"可使用\"失敗 " + server_no);
			ETL_P_Log.write_Runtime_Log("executeETL", "更新Server狀態\"可使用\"失敗 " + server_no);
			ex.printStackTrace();
			exeResult = false;
		}
		
		System.out.println("#### ETL_C_PROCESS - executeETL  End");
		
		return exeResult;
		
	}
	
	// 執行Rerun
	public static boolean executeRerun(String[] etlServerInfo, String batch_No, String central_no, String[] ptr_upload_no, Date record_Date, Date before_record_date) {
		
		System.out.println("#### ETL_C_PROCESS  Rerun  Start");
		
		String exeInfo = "Server_No : " + etlServerInfo[0] + " , Server : " + etlServerInfo[1] + " , IP : " + etlServerInfo[2] + "\n";
		exeInfo = exeInfo + "Batch_No = " + batch_No + "\n";
		exeInfo = exeInfo + "Central_no = " + central_no + "\n";
		exeInfo = exeInfo + "Record_Date = " + record_Date + "\n";
		exeInfo = exeInfo + "Before_record_date = " + before_record_date + "\n";
		
		// for test
		System.out.println(exeInfo);
		
		String server_no = etlServerInfo[0];
		
		// ETL Server下載特定中心資料
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String[] fileInfo = new String[3];
		String exc_record_dateStr = sdf.format(record_Date);
		String upload_no = "";
		String before_record_dateStr = sdf.format(before_record_date);
		boolean exeResult = true;
		
		// **更新 Server 狀態使用中
		try {
			update_Server_Status(etlServerInfo[0], "U");
		} catch (Exception ex) {
			System.out.println("更新Server狀態\"使用中\"失敗 " + server_no);
			ETL_P_Log.write_Runtime_Log("executeETL", "更新Server狀態\"使用中\"失敗 " + server_no);
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("executeETL", ex.getMessage());
			return false;
		}
		
		try {
			// 更新 新5代製作紀錄檔
			updateNewGenerationETLStatus(record_Date, central_no, "Start", "");
			
			// 呼叫ETL Server進行initial作業
			if (!ETL_C_CallWS.call_ETL_Server_initETLserver(etlServerInfo[2])) {
//				System.out.println("#### ETL_C_PROCESS - executeETL - call_ETL_Server_initETLserver 發生錯誤！ " + server_no);
//				return false;
				throw new Exception("#### ETL_C_PROCESS - executeETL - call_ETL_Server_initETLserver 發生錯誤！ " + server_no);
			}
			
			// 下載中心檔案
			ETL_Bean_Response response = ETL_C_CallWS.call_ETL_Server_getUploadFileInfo(etlServerInfo[2], central_no, "RERUN", record_Date);

			if (response.isSuccess()) {
				//取出物件轉型, <資料日期|上傳批號 |zip檔名>
				fileInfo = (String[]) response.getObj();
			}
			
			// 執行下載
			if (!response.isSuccess()) {
				throw new Exception("#### ETL_C_PROCESS - executeETL - call_ETL_Server_getUploadFileInfo 發生錯誤！ " + server_no);
			}

			upload_no = fileInfo[1];
			ptr_upload_no[0] = upload_no;
			
			System.out.println("#### ETL_C_PROCESS fileInfo[0]" + fileInfo[0] + " " + server_no);
			System.out.println("#### ETL_C_PROCESS fileInfo[1]" + fileInfo[1] + " " + server_no);
	
			// 更新報送單位狀態"使用中"
			updateRerunCentralTime(central_no, record_Date, upload_no, "Start");
			
			// 清除 ERROR_LOG_DOWNLOAD
			boolean clearErrorDowbload = true;
			clearErrorDowbload = ETL_C_CallWS.call_ETL_Server_clear_ERROR_LOG_DOWNLOAD(etlServerInfo[2]);
			
			// 寫入E Master Log
			if (!ETL_C_PROCESS.writeMasterLog(batch_No, central_no, record_Date, upload_no, "E", etlServerInfo[0])) {
				throw new Exception("E Master Log已存在\n" + exeInfo);
			};
			// 進行E系列程式
			if (!ETL_C_CallWS.call_ETL_Server_Efunction(etlServerInfo[2], "", batch_No, central_no, exc_record_dateStr, upload_no)) {
				throw new Exception("#### ETL_C_PROCESS - executeETL - call_ETL_Server_Efunction 發生錯誤！ " + server_no);
			}
			// 更新 E Master Log
			ETL_C_PROCESS.updateMasterLog(batch_No, central_no, record_Date, upload_no, "E", "E", "Y", "");
			
			// Error Log 寫入 ERROR_LOG_DOWNLOAD
			if (clearErrorDowbload) {
				ETL_C_CallWS.call_ETL_Server_write_ERROR_LOG_DOWNLOAD(etlServerInfo[2], exc_record_dateStr, central_no, upload_no);
			}
			
			// 寫入T Master Log
			if (!ETL_C_PROCESS.writeMasterLog(batch_No, central_no, record_Date, upload_no, "T", etlServerInfo[0])) {
				throw new Exception("T Master Log已存在\n" + exeInfo);
			}
			// 進行T系列程式
			if (!ETL_C_CallWS.call_ETL_Server_Tfunction(etlServerInfo[2], "", batch_No, central_no, exc_record_dateStr, upload_no, before_record_dateStr)) {
				throw new Exception("#### ETL_C_PROCESS - executeETL - call_ETL_Server_Tfunction 發生錯誤！ " + server_no);
			}
			// 更新 T Master Log
			ETL_C_PROCESS.updateMasterLog(batch_No, central_no, record_Date, upload_no, "T", "E", "Y", "");
			
			
			// 寫入L Master Log
			if (!ETL_C_PROCESS.writeMasterLog(batch_No, central_no, record_Date, upload_no, "L", etlServerInfo[0])) {
				throw new Exception("L Master Log已存在\n" + exeInfo);
			}
			// 進行L系列程式
			if (!exeLfunction(etlServerInfo[0], batch_No, central_no, exc_record_dateStr, upload_no, before_record_dateStr, "RERUN")) {
				throw new Exception("#### ETL_C_PROCESS - executeETL - exeLfunction 發生錯誤！ " + server_no);
			}
			// 更新 L Master Log
			ETL_C_PROCESS.updateMasterLog(batch_No, central_no, record_Date, upload_no, "L", "E", "Y", "");

			
    		// 執行暫存Table(load_rerun)併入五代
			addNew5G(batch_No, record_Date, central_no, upload_no, "RERUN");
			
			
			// 更新 新5代製作紀錄檔
			updateNewGenerationETLStatus(record_Date, central_no, "End", "");
			
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("executeETL", ex.getMessage());
			updateNewGenerationETLStatus(record_Date, central_no, "Error", ex.getMessage());
			exeResult = false;
		} finally {
			// 更新報送單位狀態"執行完畢"
			try {
				updateRerunCentralTime(central_no, record_Date, upload_no, "End");
			} catch (Exception ex) {
				System.out.println("更新Central:" + central_no + " - 狀態:\"執行完畢\"失敗 " + server_no);
				ETL_P_Log.write_Runtime_Log("executeETL", "更新Central:" + central_no + " - 狀態:\"執行完畢\"失敗 " + server_no);
				ex.printStackTrace();
				exeResult = false;
			}
		}
		
		// **更新 Server 狀態可使用
		try {
			update_Server_Status(etlServerInfo[0], "Y");
		} catch (Exception ex) {
			System.out.println("更新Server狀態\"可使用\"失敗 " + server_no);
			ETL_P_Log.write_Runtime_Log("executeETL", "更新Server狀態\"可使用\"失敗 " + server_no);
			ex.printStackTrace();
			exeResult = false;
		}
		
		System.out.println("#### ETL_C_PROCESS  Rerun  End");
		
		return exeResult;
		
	}
	
	// 執行Migration
	public static boolean executeMigration(String[] etlServerInfo, String batch_No, String central_no, String[] ptr_upload_no, Date record_Date, Date before_record_date) {
		
		System.out.println("#### ETL_C_PROCESS - executeMigration  Start");
		
		String exeInfo = "Server_No : " + etlServerInfo[0] + " , Server : " + etlServerInfo[1] + " , IP : " + etlServerInfo[2] + "\n";
		exeInfo = exeInfo + "Batch_No = " + batch_No + "\n";
		exeInfo = exeInfo + "Central_no = " + central_no + "\n";
		exeInfo = exeInfo + "Record_Date = " + record_Date + "\n";
		exeInfo = exeInfo + "Before_record_date = " + before_record_date + "\n";
		
		// for test
		System.out.println(exeInfo);
		
		String server_no = etlServerInfo[0];
		
		// ETL Server下載特定中心資料
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String[] fileInfo = new String[3];
		String exc_record_dateStr = sdf.format(record_Date);
		String upload_no = "";
		String before_record_dateStr = sdf.format(before_record_date);
		boolean exeResult = true;
		
		// **更新 Server 狀態使用中
		try {
			update_Server_Status(etlServerInfo[0], "U");
		} catch (Exception ex) {
			System.out.println("更新Server狀態\"使用中\"失敗 " + server_no);
			ETL_P_Log.write_Runtime_Log("executeMigration", "更新Server狀態\"使用中\"失敗 " + server_no);
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("executeMigration", ex.getMessage());
			return false;
		}
		
		try {
			// 呼叫ETL Server進行initial作業
			if (!ETL_C_CallWS.call_ETL_Server_initETLserver(etlServerInfo[2])) {
				throw new Exception("#### ETL_C_PROCESS - executeMigration - call_ETL_Server_initETLserver 發生錯誤！ " + server_no);
			}
			
			// 下載中心檔案
			ETL_Bean_Response response = ETL_C_CallWS.call_ETL_Server_getUploadMigFileInfo(etlServerInfo[2], central_no, "", null);

			if (response.isSuccess()) {
				//取出物件轉型, <資料日期|上傳批號 |zip檔名>
				fileInfo = (String[]) response.getObj();
			}
			
			// 執行下載
			if (!response.isSuccess()) {
				throw new Exception("#### ETL_C_PROCESS - executeMigration - call_ETL_Server_getUploadFileInfo 發生錯誤！ " + server_no);
			}

			upload_no = fileInfo[1];
			ptr_upload_no[0] = upload_no;
			
			System.out.println("#### ETL_C_PROCESS fileInfo[0]" + fileInfo[0] + " " + server_no);
			System.out.println("#### ETL_C_PROCESS fileInfo[1]" + fileInfo[1] + " " + server_no);

			// 清除 ERROR_LOG_DOWNLOAD
			ETL_C_CallWS.call_ETL_Server_clear_ERROR_LOG_DOWNLOAD(etlServerInfo[2]);
			
			// 寫入E Master Log
			if (!ETL_C_PROCESS.writeMasterLog(batch_No, central_no, record_Date, upload_no, "E", etlServerInfo[0])) {
				throw new Exception("E Master Log已存在\n" + exeInfo);
			};
			// 進行E系列程式
			if (!ETL_C_CallWS.call_ETL_Server_Efunction(etlServerInfo[2], "", batch_No, central_no, exc_record_dateStr, upload_no)) {
				throw new Exception("#### ETL_C_PROCESS - executeMigration - call_ETL_Server_Efunction 發生錯誤！ " + server_no);
			}
			// 更新 E Master Log
			ETL_C_PROCESS.updateMasterLog(batch_No, central_no, record_Date, upload_no, "E", "E", "Y", "");
			
			
			// 寫入T Master Log
			if (!ETL_C_PROCESS.writeMasterLog(batch_No, central_no, record_Date, upload_no, "T", etlServerInfo[0])) {
				throw new Exception("T Master Log已存在\n" + exeInfo);
			}
			// 進行T系列程式
			if (!ETL_C_CallWS.call_ETL_Server_Tfunction(etlServerInfo[2], "", batch_No, central_no, exc_record_dateStr, upload_no, before_record_dateStr)) {
				throw new Exception("#### ETL_C_PROCESS - executeMigration - call_ETL_Server_Tfunction 發生錯誤！ " + server_no);
			}
			// 更新 T Master Log
			ETL_C_PROCESS.updateMasterLog(batch_No, central_no, record_Date, upload_no, "T", "E", "Y", "");
			
			
			// 寫入L Master Log
			if (!ETL_C_PROCESS.writeMasterLog(batch_No, central_no, record_Date, upload_no, "L", etlServerInfo[0])) {
				throw new Exception("L Master Log已存在\n" + exeInfo);
			}
			// 進行L系列程式
			if (!exeLfunction(etlServerInfo[0], batch_No, central_no, exc_record_dateStr, upload_no, before_record_dateStr, "RERUN")) {
				throw new Exception("#### ETL_C_PROCESS - executeMigration - exeLfunction 發生錯誤！ " + server_no);
			}
			// 更新 L Master Log
			ETL_C_PROCESS.updateMasterLog(batch_No, central_no, record_Date, upload_no, "L", "E", "Y", "");
			
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("executeMigration", ex.getMessage());
			// 更新 新5代製作紀錄檔 - Migration Error
			updateNewGenerationMigStatus(record_Date, central_no, "Error", ex.getMessage());
			exeResult = false;
		}
		
		// **更新 Server 狀態可使用
		try {
			update_Server_Status(etlServerInfo[0], "Y");
		} catch (Exception ex) {
			System.out.println("更新Server狀態\"可使用\"失敗 " + server_no);
			ETL_P_Log.write_Runtime_Log("executeMigration", "更新Server狀態\"可使用\"失敗 " + server_no);
			ex.printStackTrace();
			exeResult = false;
		}
		
		System.out.println("#### ETL_C_PROCESS - executeMigration  End");
		
		return exeResult;
		
	}
	
	// 執行Migration Daily Run
	public static boolean executeMigETL(String[] etlServerInfo, String batch_No, String central_no, String[] ptr_upload_no, Date record_Date, Date before_record_date) {
		
		System.out.println("#### ETL_C_PROCESS - executeMigETL  Start");
		
		// for test
		System.out.println("Server_No : " + etlServerInfo[0] + " , Server : " + etlServerInfo[1] + " , IP : " + etlServerInfo[2]);
		System.out.println("Batch_No = " + batch_No);
		System.out.println("Central_no = " + central_no);
		System.out.println("Record_Date = " + record_Date);
		System.out.println("Before_record_date = " + before_record_date);
		
		String exeInfo = "Server_No : " + etlServerInfo[0] + " , Server : " + etlServerInfo[1] + " , IP : " + etlServerInfo[2] + "\n";
		exeInfo = exeInfo + "Batch_No = " + batch_No + "\n";
		exeInfo = exeInfo + "Central_no = " + central_no + "\n";
		exeInfo = exeInfo + "Record_Date = " + record_Date + "\n";
		exeInfo = exeInfo + "Before_record_date = " + before_record_date + "\n";
		
		String server_no = etlServerInfo[0];
		
		// ETL Server下載特定中心資料
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		String[] fileInfo = new String[3];
		String exc_record_dateStr = sdf.format(record_Date);
		String upload_no = "";
		String before_record_dateStr = sdf.format(before_record_date);
		boolean exeResult = true;
		
		// **更新 Server 狀態使用中
		try {
			update_Server_Status(etlServerInfo[0], "U");
		} catch (Exception ex) {
			System.out.println("更新Server狀態\"使用中\"失敗 " + server_no);
			ETL_P_Log.write_Runtime_Log("executeMigETL", "更新Server狀態\"使用中\"失敗 " + server_no);
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("executeMigETL", ex.getMessage());
			return false;
		}
		
		try {
			// 更新 新5代製作紀錄檔
			updateNewGenerationETLStatus(record_Date, central_no, "Start", "");
			
			// 呼叫ETL Server進行initial作業
			if (!ETL_C_CallWS.call_ETL_Server_initETLserver(etlServerInfo[2])) {
//					System.out.println("#### ETL_C_PROCESS - executeMigETL - call_ETL_Server_initETLserver 發生錯誤！ " + server_no);
//					return false;
				throw new Exception("#### ETL_C_PROCESS - executeMigETL - call_ETL_Server_initETLserver 發生錯誤！ " + server_no);
			}
			
			// 下載中心檔案
			ETL_Bean_Response response = ETL_C_CallWS.call_ETL_Server_getUploadFileInfo(etlServerInfo[2], central_no, "", null);

			if (response.isSuccess()) {
				//取出物件轉型, <資料日期|上傳批號 |zip檔名>
				fileInfo = (String[]) response.getObj();
			}
			
			// 執行下載
			if (!response.isSuccess()) {
				throw new Exception("#### ETL_C_PROCESS - executeMigETL - call_ETL_Server_getUploadFileInfo 發生錯誤！ " + server_no);
			}

			upload_no = fileInfo[1];
			ptr_upload_no[0] = upload_no;
			
			System.out.println("#### ETL_C_PROCESS fileInfo[0]" + fileInfo[0] + " " + server_no);
			System.out.println("#### ETL_C_PROCESS fileInfo[1]" + fileInfo[1] + " " + server_no);
	
			// 更新報送單位狀態"使用中"
			updateRerunCentralTime(central_no, record_Date, upload_no, "Start");
			
			// 寫入E Master Log
			if (!ETL_C_PROCESS.writeMasterLog(batch_No, central_no, record_Date, upload_no, "E", etlServerInfo[0])) {
				throw new Exception("E Master Log已存在\n" + exeInfo);
			};
			// 進行E系列程式
			if (!ETL_C_CallWS.call_ETL_Server_Efunction(etlServerInfo[2], "", batch_No, central_no, exc_record_dateStr, upload_no)) {
				throw new Exception("#### ETL_C_PROCESS - executeMigETL - call_ETL_Server_Efunction 發生錯誤！ " + server_no);
			}
			// 更新 E Master Log
			ETL_C_PROCESS.updateMasterLog(batch_No, central_no, record_Date, upload_no, "E", "E", "Y", "");
			
			// Error Log 寫入 ERROR_LOG_DOWNLOAD
			ETL_C_CallWS.call_ETL_Server_write_ERROR_LOG_DOWNLOAD(etlServerInfo[2], exc_record_dateStr, central_no, upload_no);
			
			// 寫入T Master Log
			if (!ETL_C_PROCESS.writeMasterLog(batch_No, central_no, record_Date, upload_no, "T", etlServerInfo[0])) {
				throw new Exception("T Master Log已存在\n" + exeInfo);
			}
			// 進行T系列程式
			if (!ETL_C_CallWS.call_ETL_Server_Tfunction(etlServerInfo[2], "", batch_No, central_no, exc_record_dateStr, upload_no, before_record_dateStr)) {
				throw new Exception("#### ETL_C_PROCESS - executeMigETL - call_ETL_Server_Tfunction 發生錯誤！ " + server_no);
			}
			// 更新 T Master Log
			ETL_C_PROCESS.updateMasterLog(batch_No, central_no, record_Date, upload_no, "T", "E", "Y", "");
			
			
			// 寫入L Master Log
			if (!ETL_C_PROCESS.writeMasterLog(batch_No, central_no, record_Date, upload_no, "L", etlServerInfo[0])) {
				throw new Exception("L Master Log已存在\n" + exeInfo);
			}
			// 進行L系列程式
			if (!exeLfunction(etlServerInfo[0], batch_No, central_no, exc_record_dateStr, upload_no, before_record_dateStr, "RERUN")) {
				throw new Exception("#### ETL_C_PROCESS - executeMigETL - exeLfunction 發生錯誤！ " + server_no);
			}
			// 更新 L Master Log
			ETL_C_PROCESS.updateMasterLog(batch_No, central_no, record_Date, upload_no, "L", "E", "Y", "");

			
			// 執行暫存Table(load_temp)併入五代
			addNew5G(batch_No, record_Date, central_no, upload_no, "RERUN");
			
			
			// 更新 新5代製作紀錄檔
//			updateNewGenerationETLStatus(record_Date, central_no, "End", "");
			updateNewGenerationETLStatus(record_Date, central_no, "Mid", "");
			
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("executeMigETL", ex.getMessage());
			// 更新ETL  執行狀態
			updateNewGenerationETLStatus(record_Date, central_no, "Error", ex.getMessage());
			// 更新 新5代製作紀錄檔 - Migration Error
			updateNewGenerationMigStatus(record_Date, central_no, "Error", ex.getMessage());
			exeResult = false;
		} finally {
			// 更新報送單位狀態"執行完畢"
			try {
				updateRerunCentralTime(central_no, record_Date, upload_no, "End");
			} catch (Exception ex) {
				System.out.println("更新Central:" + central_no + " - 狀態:\"執行完畢\"失敗 " + server_no);
				ETL_P_Log.write_Runtime_Log("executeMigETL", "更新Central:" + central_no + " - 狀態:\"執行完畢\"失敗 " + server_no);
				ex.printStackTrace();
				exeResult = false;
			}
		}
		
		// **更新 Server 狀態可使用
		try {
			update_Server_Status(etlServerInfo[0], "Y");
		} catch (Exception ex) {
			System.out.println("更新Server狀態\"可使用\"失敗 " + server_no);
			ETL_P_Log.write_Runtime_Log("executeMigETL", "更新Server狀態\"可使用\"失敗 " + server_no);
			ex.printStackTrace();
			exeResult = false;
		}
		
		System.out.println("#### ETL_C_PROCESS - executeMigETL  End");
		
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
//			    throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            ETL_P_Log.write_Runtime_Log("writeMasterLog", "Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            
	            return false;
			}
			
			return true;
	        
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("writeMasterLog", ex.getMessage());
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
//			    throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            ETL_P_Log.write_Runtime_Log("updateMasterLog", "Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            
	            return false;
			}
			
			return true;
	        
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("updateMasterLog", ex.getMessage());
			return false;
		}
		
	}
	
	// 更新 Server 狀態使用中
	public static void update_Server_Status(String server_No, String usable_Status) throws Exception {
		
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
			ETL_P_Log.write_Runtime_Log("update_Server_Status", "Error Code = " + returnCode + ", Error Message : " + errorMessage);
			throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
		}
			
	}
	
	// 更新報送單位狀態
	private static void updateCentralTime(String central_No, Date record_Date, String upload_No, String status) throws Exception {
		
		//for test
//		System.out.println("##########update_Central_Time Start");
//		System.out.println("central_No:" + central_No);
//		System.out.println("record_Date:" + record_Date);
//		System.out.println("upload_No:" + upload_No);
//		System.out.println("status:" + status);
//		System.out.println("##########update_Central_Time End");
		
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
			ETL_P_Log.write_Runtime_Log("updateCentralTime", "Error Code = " + returnCode + ", Error Message : " + errorMessage);
			throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
		}
		
	}
	
	// 更新報送單位狀態
	private static void updateRerunCentralTime(String central_No, Date record_Date, String upload_No, String status) throws Exception {
		
		//for test
//		System.out.println("##########update_Central_Time Start");
//		System.out.println("central_No:" + central_No);
//		System.out.println("record_Date:" + record_Date);
//		System.out.println("upload_No:" + upload_No);
//		System.out.println("status:" + status);
//		System.out.println("##########update_Central_Time End");
		
		String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.update_RerunCentral_Time(?,?,?,?,?,?)}";
		
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
//				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			ETL_P_Log.write_Runtime_Log("updateCentralTime", "Error Code = " + returnCode + ", Error Message : " + errorMessage);
			throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
		}
		
	}
	
	// 執行L系列程式
	private static boolean exeLfunction(String server_No, String batch_No, String exc_central_no, String record_DateStr,
			String upload_No, String before_record_dateStr, String tableType) {
		
		System.out.println("call_ETL_Server_Lfunction : 開始執行  " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
		try {

			ETL_Bean_LogData logData = new ETL_Bean_LogData();
			logData.setBATCH_NO(batch_No);
			logData.setCENTRAL_NO(exc_central_no);
			logData.setFILE_TYPE("");
			Date exc_record_date = new SimpleDateFormat("yyyyMMdd").parse(record_DateStr);
			logData.setRECORD_DATE(exc_record_date);
			logData.setUPLOAD_NO(upload_No);
			Date before_record_date = new SimpleDateFormat("yyyyMMdd").parse(before_record_dateStr);
			logData.setBEFORE_ETL_PROCESS_DATE(before_record_date);

			String fedServer = "";
			// 設定fedName
			if ("ETL_S1".equals(server_No)) {
				fedServer = "ETLDB001";
			} else if ("ETL_S2".equals(server_No)) {
				fedServer = "ETLDB002";
			}

			// 執行用Table (正常 temp, 重跑rerun)
			String runTable = tableType;
//			String runTable = "rerun"; // test  temp  2018.04.23  TimJhang
			
			
			System.out.println("batch_No:" + batch_No);
			System.out.println("exc_central_no:" + exc_central_no);
			System.out.println("exc_record_date:" + exc_record_date);
			System.out.println("upload_No:" + upload_No);
			System.out.println("server_No:" + server_No);
			System.out.println("runTable:" + runTable);

			// 執行20支L系列程式
//			if ("018".equals(exc_central_no.trim())) {
//				ETL_Bean_LogData logData2 = logData.clone();
//				
//				List<String> central_list = getUsableCentralList(exc_record_date);
//				
//				for (int i = 0; i < central_list.size(); i++) {
//					logData2.setCENTRAL_NO(central_list.get(i));
//					
//					// 更新7個單位日曆檔 & 匯率檔
//					logData2.setPROGRAM_NO("ETL_L_CALENDAR");
//					new ETL_L_CALENDAR().trans_to_CALENDAR_LOAD(logData2, fedServer, runTable);
//					
//					logData2.setPROGRAM_NO("ETL_L_FX_RATE");
//					new ETL_L_FX_RATE().trans_to_FX_RATE_LOAD(logData2, fedServer, runTable);
//				}
//				
//				// 更新GAMLDB上的日曆檔(Calendar)
//				copyCalendar_ToGAMLDB();
//			}
//			
//			logData.setPROGRAM_NO("ETL_L_PARTY_PHONE");
//			new ETL_L_PARTY_PHONE().trans_to_PARTY_PHONE_LOAD(logData, fedServer, runTable);
//			
//			logData.setPROGRAM_NO("ETL_L_COLLATERAL");
//			new ETL_L_COLLATERAL().trans_to_COLLATERAL_LOAD(logData, fedServer, runTable);
//			
//			logData.setPROGRAM_NO("ETL_L_LOAN_DETAIL");
//			new ETL_L_LOAN_DETAIL().trans_to_LOAN_DETAIL_LOAD(logData, fedServer, runTable);
//			
//			logData.setPROGRAM_NO("ETL_L_LOAN_GUARANTOR");
//			new ETL_L_LOAN_GUARANTOR().trans_to_LOAN_GUARANTOR_LOAD(logData, fedServer, runTable);
//			
//			logData.setPROGRAM_NO("ETL_L_LOAN_MASTER");
//			new ETL_L_LOAN_MASTER().trans_to_LOAN_MASTER_LOAD(logData, fedServer, runTable);
//			
//			logData.setPROGRAM_NO("ETL_L_LOAN");
//			new ETL_L_LOAN().trans_to_LOAN_LOAD(logData, fedServer, runTable);
//			
//			logData.setPROGRAM_NO("ETL_L_ACCOUNT_PROPERTY");
//			new ETL_L_ACCOUNT_PROPERTY().trans_to_ACCOUNT_PROPERTY_LOAD(logData, fedServer, runTable);
//			
//			logData.setPROGRAM_NO("ETL_L_ACCOUNT");
//			new ETL_L_ACCOUNT().trans_to_ACCOUNT_LOAD(logData, fedServer, runTable);
//			
//			logData.setPROGRAM_NO("ETL_L_BALANCE");
//			new ETL_L_BALANCE().trans_to_BALANCE_LOAD(logData, fedServer, runTable);
//			
//			logData.setPROGRAM_NO("ETL_L_PARTY_ACCOUNT_REL");
//			new ETL_L_PARTY_ACCOUNT_REL().trans_to_PARTY_ACCOUNT_REL_LOAD(logData, fedServer, runTable);
//			
//			logData.setPROGRAM_NO("ETL_L_PARTY_ADDRESS");
//			new ETL_L_PARTY_ADDRESS().trans_to_PARTY_ADDRESS_LOAD(logData, fedServer, runTable);
//			
//			logData.setPROGRAM_NO("ETL_L_PARTY_EMAIL");
//			new ETL_L_PARTY_EMAIL().trans_to_PARTY_EMAIL_LOAD(logData, fedServer, runTable);
//			
//			logData.setPROGRAM_NO("ETL_L_PARTY_NATINOALITY");
//			new ETL_L_PARTY_NATINOALITY().trans_to_PARTY_NATINOALITY_LOAD(logData, fedServer, runTable);
//			
//			logData.setPROGRAM_NO("ETL_L_PARTY_PARTY_REL");
//			new ETL_L_PARTY_PARTY_REL().trans_to_PARTY_PARTY_REL_LOAD(logData, fedServer, runTable);
//			
//			logData.setPROGRAM_NO("ETL_L_PARTY");
//			new ETL_L_PARTY().trans_to_PARTY_LOAD(logData, fedServer, runTable);
//			
//			logData.setPROGRAM_NO("ETL_L_SERVICE");
//			new ETL_L_SERVICE().trans_to_SERVICE_LOAD(logData, fedServer, runTable);
//			
//			logData.setPROGRAM_NO("ETL_L_TRANSFER");
//			new ETL_L_TRANSFER().trans_to_TRANSFER_LOAD(logData, fedServer, runTable);
//			
//			logData.setPROGRAM_NO("ETL_L_TRANSACTION");
//			new ETL_L_TRANSACTION().trans_to_TRANSACTION_LOAD(logData, fedServer, runTable);
//			
//			logData.setPROGRAM_NO("ETL_L_ERROR_LOG");
//			new ETL_L_ERROR_LOG().trans_to_Error_Log(logData, fedServer, runTable);
			
			
			/** 平行化區塊  Start **/
			// 執行20支L系列程式
			List<Load> loads = new ArrayList<Load>();
			
			// clone 新class指標用
			ETL_Bean_LogData logData2;
			
			logData2 = logData.clone();
			logData2.setPROGRAM_NO("ETL_L_TRANSACTION");
			loads.add(new ETL_L_TRANSACTION(logData2, fedServer, runTable));
			
			logData2 = logData.clone();
			logData2.setPROGRAM_NO("ETL_L_PARTY_PHONE");
			loads.add(new ETL_L_PARTY_PHONE(logData2, fedServer, runTable));
			
			logData2 = logData.clone();
			logData2.setPROGRAM_NO("ETL_L_COLLATERAL");
			loads.add(new ETL_L_COLLATERAL(logData2, fedServer, runTable));
			
			logData2 = logData.clone();
			logData2.setPROGRAM_NO("ETL_L_LOAN_DETAIL");
			loads.add(new ETL_L_LOAN_DETAIL(logData2, fedServer, runTable));

			logData2 = logData.clone();
			logData2.setPROGRAM_NO("ETL_L_LOAN_GUARANTOR");
			loads.add(new ETL_L_LOAN_GUARANTOR(logData2, fedServer, runTable));

			logData2 = logData.clone();
			logData2.setPROGRAM_NO("ETL_L_LOAN_MASTER");
			loads.add(new ETL_L_LOAN_MASTER(logData2, fedServer, runTable));

			logData2 = logData.clone();
			logData2.setPROGRAM_NO("ETL_L_LOAN");
			loads.add(new ETL_L_LOAN(logData2, fedServer, runTable));

			logData2 = logData.clone();
			logData2.setPROGRAM_NO("ETL_L_ACCOUNT_PROPERTY");
			loads.add(new ETL_L_ACCOUNT_PROPERTY(logData2, fedServer, runTable));

			logData2 = logData.clone();
			logData2.setPROGRAM_NO("ETL_L_ACCOUNT");
			loads.add(new ETL_L_ACCOUNT(logData2, fedServer, runTable));

			logData2 = logData.clone();
			logData2.setPROGRAM_NO("ETL_L_BALANCE");
			loads.add(new ETL_L_BALANCE(logData2, fedServer, runTable));

			logData2 = logData.clone();
			logData2.setPROGRAM_NO("ETL_L_PARTY_ACCOUNT_REL");
			loads.add(new ETL_L_PARTY_ACCOUNT_REL(logData2, fedServer, runTable));

			logData2 = logData.clone();
			logData2.setPROGRAM_NO("ETL_L_PARTY_ADDRESS");
			loads.add(new ETL_L_PARTY_ADDRESS(logData2, fedServer, runTable));

			logData2 = logData.clone();
			logData2.setPROGRAM_NO("ETL_L_PARTY_EMAIL");
			loads.add(new ETL_L_PARTY_EMAIL(logData2, fedServer, runTable));

			// 2018.06.27  國籍檔確認取消使用 James
//			logData2 = logData.clone();
//			logData2.setPROGRAM_NO("ETL_L_PARTY_NATINOALITY");
//			loads.add(new ETL_L_PARTY_NATINOALITY(logData2, fedServer, runTable));

			logData2 = logData.clone();
			logData2.setPROGRAM_NO("ETL_L_PARTY_PARTY_REL");
			loads.add(new ETL_L_PARTY_PARTY_REL(logData2, fedServer, runTable));

			logData2 = logData.clone();
			logData2.setPROGRAM_NO("ETL_L_PARTY");
			loads.add(new ETL_L_PARTY(logData2, fedServer, runTable));

			logData2 = logData.clone();
			logData2.setPROGRAM_NO("ETL_L_SERVICE");
			loads.add(new ETL_L_SERVICE(logData2, fedServer, runTable));

			logData2 = logData.clone();
			logData2.setPROGRAM_NO("ETL_L_TRANSFER");
			loads.add(new ETL_L_TRANSFER(logData2, fedServer, runTable));
			
			logData2 = logData.clone();
			logData2.setPROGRAM_NO("ETL_L_AGENT");
			loads.add(new ETL_L_AGENT(logData2, fedServer, runTable));
			
			logData2 = logData.clone();
			logData2.setPROGRAM_NO("ETL_L_SCUSTBOXOPEN");
			loads.add(new ETL_L_SCUSTBOXOPEN(logData2, fedServer, runTable));
			
			logData2 = logData.clone();
			logData2.setPROGRAM_NO("ETL_L_ERROR_LOG");
			loads.add(new ETL_L_ERROR_LOG(logData2, fedServer, runTable));
			
			if ("018".equals(exc_central_no.trim())) {
				
				List<String> central_list = getUsableCentralList(exc_record_date);
				
				for (int i = 0; i < central_list.size(); i++) {
					
					// 更新7個單位日曆檔 & 匯率檔
					logData2 = logData.clone();
					logData2.setCENTRAL_NO(central_list.get(i));
					logData2.setPROGRAM_NO("ETL_L_CALENDAR");
					loads.add(new ETL_L_CALENDAR(logData2, fedServer, runTable));
					
					logData2 = logData.clone();
					logData2.setCENTRAL_NO(central_list.get(i));
					logData2.setPROGRAM_NO("ETL_L_FX_RATE");
					loads.add(new ETL_L_FX_RATE(logData2, fedServer, runTable));
				}
			}
			
			ExecutorService executor = Executors.newFixedThreadPool(5);
			
			for (Load load : loads) {
				executor.execute(load);
			}
			
			executor.shutdown();

			while (!executor.isTerminated()) {
			}
			
			System.out.println("線程池已經關閉");
			
			if ("018".equals(exc_central_no.trim())) {
				// 更新GAMLDB上的日曆檔(Calendar)
				copyCalendar_ToGAMLDB();
			}
			/** 平行化區塊  End **/
			
			// 匯率檔檢查補漏
			supplementFX_Rate(logData);
			
			// 擔保品, 保證人檔對party進行補漏作業
			ETL_C_Comm.SupplementParty(logData, fedServer, runTable);
			
			// 修正代理人寫入party檔Branch Code
			ETL_C_Comm.ModifyPartyBranchCodeFromAgent(logData, fedServer, runTable);

		} catch (Exception ex) {
			System.out.println("call_ETL_Server_Lfunction : 發生錯誤");
			ETL_P_Log.write_Runtime_Log("exeLfunction", "call_ETL_Server_Lfunction : 發生錯誤");
			ex.printStackTrace();
			return false;
		}
		
		System.out.println("call_ETL_Server_Lfunction : 結束  " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
		return true;
	}
	
	// 更新5代Table 記錄檔  prepare status
	public static boolean updateNewGenerationETLStatus(Date record_date, String central_no, String etlStatus, String discription) {
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.update_New_Generation_ETL_Status(?,?,?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(record_date.getTime()));
			cstmt.setString(3, central_no);
			cstmt.setString(4, etlStatus);
			cstmt.setString(5, discription);
			cstmt.registerOutParameter(6, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(6);
	            System.out.println("####updateNewGenerationETLStatus - Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            ETL_P_Log.write_Runtime_Log("updateNewGenerationETLStatus", "####updateNewGenerationETLStatus - Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            return false;
			}
			
			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("updateNewGenerationETLStatus", ex.getMessage());
			return false;
		}
	}
	
	// 取得需進行ETL作業中心代號
	private static List<String> getUsableCentralList(Date recordDate) {
		List<String> resultList = new ArrayList<String>();
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.getUsableCentral(?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(recordDate.getTime()));
			cstmt.registerOutParameter(3, DB2Types.CURSOR);
			cstmt.registerOutParameter(4, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(4);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
//	            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            ETL_P_Log.write_Runtime_Log("getUsableCentralList", "Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            return resultList;
			}
			
			java.sql.ResultSet rs = (java.sql.ResultSet) cstmt.getObject(3);
			while (rs.next()) {
	        	String central_no = rs.getString(1);
	        	if (central_no != null) {
	        		central_no = central_no.trim();
	        	}
	        	resultList.add(central_no);
	        }
	        
//	        System.out.println("List Size = " + resultList.size()); // for test
		
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("getUsableCentralList", ex.getMessage());
		}
		
		return resultList;
	}
	
	// 匯率檔檢查, 自動補充
	private static void supplementFX_Rate(ETL_Bean_LogData logData) {
		System.out.println("#######Load - supplementFX_Rate - Start");
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Load.supplementFX_Rate(?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection(logData.getCENTRAL_NO().trim());
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(logData.getRECORD_DATE().getTime()));
			cstmt.registerOutParameter(3, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(3);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            ETL_P_Log.write_Runtime_Log("supplementFX_Rate", "Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		System.out.println("#######Load - supplementFX_Rate - End");
	}
	
	// 用GAML018上的日曆檔, 更新GAMLDB上的日曆檔
	private static void copyCalendar_ToGAMLDB() {
		System.out.println("#######Load - copyCalendar_ToGAMLDB - Start");
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".GAMLDB.copyCalendar_ToGAMLDB(?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.registerOutParameter(2, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(2);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            ETL_P_Log.write_Runtime_Log("copyCalendar_ToGAMLDB", "Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		System.out.println("#######Load - copyCalendar_ToGAMLDB - End");
	}
	
	// 確認ETL是否執行正確無誤
	private static boolean checkETLright(String batch_no, Date record_date, String central_no, String upload_no) {
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.guery_ETL_Run_Result(?,?,?,?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setString(2, batch_no);
			cstmt.setDate(3, new java.sql.Date(record_date.getTime()));
			cstmt.setString(4, central_no);
			cstmt.setString(5, upload_no);
			cstmt.registerOutParameter(6, Types.VARCHAR);
			cstmt.registerOutParameter(7, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(7);
	            System.out.println("####checkETLright - Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            ETL_P_Log.write_Runtime_Log("checkETLright", "####checkETLright - Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            return false;
			}
			
			String right = cstmt.getString(6);
			if ("Y".equals(right)) {
				return true;
			} else {
				return false;
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("checkETLright", ex.getMessage());
			return false;
		}
	}
	
	// 執行暫存Table(load_temp)併入五代
	private static void addNew5G(String batch_no, Date record_date, String central_no, String upload_no, String tableType) throws Exception {
		
		try {
			// test  temp
			// 確認ETL是否執行正確無誤
//			if (!checkETLright(batch_no, record_date, central_no, upload_no)) {
//				System.out.println("單位:" + central_no + " , 資料日期:" + new SimpleDateFormat("yyyyMMdd").format(record_date) + " , 上傳批號:" + upload_no + 
//						"\nETL處理中有出現錯誤，不執行五代合併程式!");
//				return false;
//			}
			
			// 查詢partition狀況, 取得分割數量(5個以上才做處理) & 最早的檔名(yyyyMMdd)
			String[] partition_Info = new String[3];
			if (!guery_GAML_Partition_Info(central_no, partition_Info)) {
				System.out.println("取得partition訊息出現錯誤，返回不繼續執行！");
				ETL_P_Log.write_Runtime_Log("addNew5G", "取得partition訊息出現錯誤，返回不繼續執行！");
				throw new Exception("取得partition訊息出現錯誤，返回不繼續執行！");
			}
			
			// 欲drop資料日期
			Date dropDate = null;
			
			int Gcount = 5;
			if (isFormal) {
				Gcount = 5;
			} else {
				Gcount = 10;
			}
			
//			if (Integer.valueOf(partition_Info[0]) >= 6) { // for test
			if (Integer.valueOf(partition_Info[0]) >= Gcount) {
				dropDate = ETL_Tool_StringX.toUtilDate(partition_Info[1]);
			}
			
			// 執行併回
			ETL_C_FIVE_G.renew5GTable(dropDate, record_date, central_no, tableType);
			
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("addNew5G", ex.getMessage());
			throw new Exception(ex.getMessage());
		}
	}
	
	// 查詢partition狀況, 取得分割數量(5個以上才做處理) & 最早的檔名(yyyyMMdd)
	private static boolean guery_GAML_Partition_Info (String central_no, String[] partition_Info) {
		
		try {
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.guery_GAML_Partition_Info(?,?,?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setString(2, central_no);
			cstmt.registerOutParameter(3, Types.INTEGER);
			cstmt.registerOutParameter(4, Types.VARCHAR);
			cstmt.registerOutParameter(5, Types.VARCHAR);
			cstmt.registerOutParameter(6, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(6);
	            System.out.println("####guery_GAML_Partition_Info - Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            ETL_P_Log.write_Runtime_Log("guery_GAML_Partition_Info", "####guery_GAML_Partition_Info - Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            return false;
			}
			
			int partiCount = cstmt.getInt(3);
			String partiFirstName = cstmt.getString(4);
			String partiLastName = cstmt.getString(5);
			
			System.out.println("partiCount = " + partiCount);
			System.out.println("partiFirstName = " + partiFirstName);
			System.out.println("partiLastName = " + partiLastName);
			
			partition_Info[0] = String.valueOf(partiCount);
			partition_Info[1] = partiFirstName;
			partition_Info[2] = partiLastName;
			
			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("guery_GAML_Partition_Info", ex.getMessage());
			return false;
		}
	}
	
	public static boolean updateNewGenerationMigStatus(Date record_date, String central_no, String mig_Status, String discription) {
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Migration.update_New_Generation_MigStatus(?,?,?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(record_date.getTime()));
			cstmt.setString(3, central_no);
			cstmt.setString(4, mig_Status);
			cstmt.setString(5, discription);
			cstmt.registerOutParameter(6, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(6);
	            System.out.println("####updateNewGenerationMigStatus - Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            ETL_P_Log.write_Runtime_Log("updateNewGenerationMigStatus", "####updateNewGenerationMigStatus - Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            return false;
			}
			
			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("updateNewGenerationETLStatus", ex.getMessage());
			return false;
		}
	}
	
	public static void main(String[] argv) {
		try {
			
			System.out.println("test Start");
			
			updateNewGenerationMigStatus(new SimpleDateFormat("yyyyMMdd").parse("20180608"), "600", "Wait", "");
			
			System.out.println("test End");
			
//			copyCalendar_ToGAMLDB();
			
//			String[] dateAry = new String[3];
//			guery_GAML_Partition_Info("018", dateAry);
//			System.out.println(dateAry[0]);
//			System.out.println(dateAry[1]);
//			System.out.println(dateAry[2]);
			
//			String[] serverInfo = new String[3];
//			serverInfo[0] = "ETL_S1";
//			serverInfo[1] = "test2";
//			serverInfo[2] = "127.0.0.1:8083";
//			Date date = new Date();
//			Date b_date = new Date();
//			String[] ptr_upload_no = new String[1];
//			executeETL(serverInfo, "tim18226", "600", ptr_upload_no, date, b_date);
			
//			ETL_Bean_LogData logData = new ETL_Bean_LogData();
//			logData.setCENTRAL_NO("952");
//			logData.setRECORD_DATE(new SimpleDateFormat("yyyyMMdd").parse("20180530"));
//			supplementFX_Rate(logData);
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
	}

}
