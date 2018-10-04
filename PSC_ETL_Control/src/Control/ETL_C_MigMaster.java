package Control;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import com.ibm.db2.jcc.DB2Types;

import Bean.ETL_Bean_MigrationUnit;
import Control.ETL_C_Profile;
import DB.ConnectionHelper;
import DB.ETL_P_Log;
import DB.ETL_P_RunState;
import FTP.ETL_SFTP;
import Profile.ETL_Profile;
import Tool.ETL_Tool_FormatCheck;
import Tool.ETL_Tool_Mail;

public class ETL_C_MigMaster {
	
	// 是否為正式參數
	private static boolean isFormal = true;

	// 固定每日00:01開始執行程式
	public static void execute() {
		
		Calendar c1 = Calendar.getInstance();
    	final String strTime = String.format("%1$tH%1$tM", c1);
		
    	//  使用編號" 1"設定檔(BatchRunTimeConfig)
    	boolean isRun = false;
    	String runBatchNo = "";
    	
		// 正式版本
		runBatchNo = "10";
		isRun = ETL_C_BatchTime.isExecute(strTime, runBatchNo);
    	
		if (!isRun) {
//			System.out.println("isRun = false  " + runBatchNo);
			return;
		}
		
		// 若Rerun執行則, 則等待
    	if (isRerunExecute()) {
//    		System.out.println("####ETL_C_MigMaster - Rerun 作業進行, 不進行ETL作業。  " + runBatchNo);
//    		ETL_P_Log.write_Runtime_Log("ETL_C_MigMaster", "####ETL_C_MigMaster - Rerun 作業進行, 不進行ETL作業。  " + runBatchNo);
    		return;
    	}
    	
    	// 若ETL計畫執行中, 則等待
    	if (ETL_C_Migration.isETLplanExecute(new String[1], new String[1])) {
//    		System.out.println("####ETL_C_MigMaster - ETL計畫執行中, 不進行MigETL作業。  " + runBatchNo);
//    		ETL_P_Log.write_Runtime_Log("ETL_C_MigMaster", "####ETL_C_MigMaster - ETL計畫執行中, 不進行MigETL作業。  " + runBatchNo);
    		return;
    	}
    	
    	// 取得執行中心代號(序號表ETL_CENTRAL_INFO), 取得執行完Migration中心資料
		List<ETL_Bean_MigrationUnit> needMigETLCentralList = getMidMigrationList();
 		if (needMigETLCentralList.size() == 0) {
 			return;
 		}
 		// 資料日期
 		Date record_date = needMigETLCentralList.get(0).getRecordDate();
 		List<String> migETLCentralList = new ArrayList<String>(1);
 		migETLCentralList.add(needMigETLCentralList.get(0).getCentralNo());
 		
 		Date before_record_date;
 		try {
        	before_record_date = getBeforeRecordDate(record_date);
        } catch (Exception ex) {
        	ex.printStackTrace();
        	System.out.println("#### ETL_C_MigMaster - 無法取得上一個資料日期，無法繼續進行！");
        	ETL_P_Log.write_Runtime_Log("ETL_C_MigMaster", "#### ETL_C_MigMaster - 無法取得上一個資料日期，無法繼續進行！");
        	return;
        }
 		
		System.out.println("#### ETL_C_MigMaster Start  " + runBatchNo);
		
		/* 取得可用ETL_Server資訊(資訊表ETL_SERVER_INFO), 檢查過濾ETL_Server是否正常可用 */
		// 如果具有可用ETL Server, 才開始進行, 否則這一輪結束
		
		// 取得可用ETL Server
		List<String[]> etlServerList = getUsableETLServer("ETL_SERVER", "Y");
		
		// 檢核ETL Server 是否正常可連線
		// 排除連線異常，不可使用ETL Server，並給出提示訊息
		filterETLServerOK(etlServerList);
		
		if (etlServerList.size() == 0) {
			System.out.println("#### 無可用ETL Server 不進行作業");
			ETL_P_Log.write_Runtime_Log("ETL_C_MigMaster", "#### 無可用ETL Server 不進行作業");
			return;
		}
		
		// 呼叫確認用Web Service連線
 		System.out.println("etlServerList size = " + etlServerList.size()); // for test
		System.out.println("Usable ETL Server List :");
		for (int i = 0; i < etlServerList.size(); i++) {
			System.out.println("Server_No : " + etlServerList.get(i)[0] + " , Server : " + etlServerList.get(i)[1] + " , IP : " + etlServerList.get(i)[2]);
		}
        
        System.out.println("讀取資料日期：" + new SimpleDateFormat("yyyyMMdd").format(record_date)); // for test
		
		// 取得批次編號(序號表ETL_PARAMETER_INFO)
		String batchNo = getETL_BatchNo();
		System.out.println("BatchNo = " + batchNo); // for test
		ETL_P_Log.write_Runtime_Log("ETL_C_MigMaster", "BatchNo = " + batchNo);
		
		// 掃描中心SFTP今日檔案是否已上傳
		List<String> readyCentralList = checkReadyCentral(migETLCentralList, record_date, batchNo);
		System.out.println("readyCentralList size = " + readyCentralList.size()); // for test
		ETL_P_Log.write_Runtime_Log("ETL_C_MigMaster", "readyCentralList size = " + readyCentralList.size());
		
		if (readyCentralList.size() == 0) {
			System.out.println("#### readyCentralList 無需要處理中心");
			ETL_P_Log.write_Runtime_Log("ETL_C_MigMaster", "#### readyCentralList 無需要處理中心");
			return;
		}
		System.out.println("ReadyCentralList :");
		ETL_P_Log.write_Runtime_Log("ETL_C_MigMaster", "ReadyCentralList :");
		for (int i = 0; i < readyCentralList.size(); i++) {
			System.out.println(readyCentralList.get(i));
			ETL_P_Log.write_Runtime_Log("ETL_C_MigMaster", readyCentralList.get(i));
		}
		
		// 標記為Migration ETL開始
		ETL_C_PROCESS.updateNewGenerationMigStatus(record_date, readyCentralList.get(0), "MidStart", "");
		
		String[] ptr_upload_no = new String[1];
		
		boolean etlSuccess = true;
		if (isFormal) {
			// 指定ETL任務
			etlSuccess = ETL_C_PROCESS.executeMigETL(etlServerList.get(0), batchNo, readyCentralList.get(0), ptr_upload_no, record_date, before_record_date);
		}
		
		// 執行正常完整, 則寫入
		if (etlSuccess) {
			
			if (isFormal) {
				// 執行runstate程式
				ETL_P_RunState.runStateStart(readyCentralList.get(0));
			} else {
				// 執行runstate程式
//				runStateSRC(readyCentralList.get(0));
			}
			
			if (isFormal) {
				// 寫入ETL完成紀錄ETL_LOAD_GAML
				boolean isSuccess = write_ETL_LOAD_GAML(batchNo, record_date, readyCentralList.get(0), ptr_upload_no[0]);
				
				System.out.println("batchNo = " + batchNo + " , record_date = " + record_date 
						+ " , central_no = " + readyCentralList.get(0) + " , upload_no = " + ptr_upload_no[0]);
				if (isSuccess) {
					System.out.println("寫入ETL_LOAD_GAML成功!");
					ETL_P_Log.write_Runtime_Log("ETL_C_MigMaster", "寫入ETL_LOAD_GAML成功!");
					
					// 寫入執行成功信件
					String mailContent = 
							"單位：" + readyCentralList.get(0) + "  資料日期：" + new SimpleDateFormat("yyyyMMdd").format(record_date) +
							"  執行DataMigration - 每日異動完畢！";
					ETL_Tool_Mail.writeAML_Mail(readyCentralList.get(0), "SYSAdmin", null, "AML_ETL 系統通知", mailContent);
				} else {
					System.out.println("寫入ETL_LOAD_GAML失敗!");
					ETL_P_Log.write_Runtime_Log("ETL_C_MigMaster", "寫入ETL_LOAD_GAML失敗!");
					
					// 寫入信件
					String mailContent = 
							"單位：" + readyCentralList.get(0) + "  資料日期：" + new SimpleDateFormat("yyyyMMdd").format(record_date) +
							"  執行DataMigration - 每日異動啟動CDD/TMS失敗，請洽IT人員。";
					ETL_Tool_Mail.writeAML_Mail(readyCentralList.get(0), "SYSAdmin", null, "AML_ETL 系統通知", mailContent);
				}
				
			} else {
				// 寫入ETL完成紀錄ETL_LOAD_GAML
				boolean isSuccess = write_ETL_LOAD_GAML(batchNo, record_date, readyCentralList.get(0), ptr_upload_no[0]);
				
				System.out.println("batchNo = " + batchNo + " , record_date = " + record_date 
						+ " , central_no = " + readyCentralList.get(0) + " , upload_no = " + ptr_upload_no[0]);
				if (isSuccess) {
					System.out.println("寫入ETL_LOAD_GAML成功!");
					ETL_P_Log.write_Runtime_Log("ETL_C_MigMaster", "寫入ETL_LOAD_GAML成功!");
				} else {
					System.out.println("寫入ETL_LOAD_GAML失敗!");
					ETL_P_Log.write_Runtime_Log("ETL_C_MigMaster", "寫入ETL_LOAD_GAML失敗!");
				}
				
//				System.out.println("跳過ETL_LOAD_GAML寫入，因為是全量！");
//				System.out.println("跳過ETL_LOAD_GAML寫入，因為需檢查資料正確性！");
			}
			
		} else {
			// 更新5代紀錄Table MigStatus 註記為第二階段錯誤
			ETL_C_PROCESS.updateNewGenerationMigStatus(record_date, readyCentralList.get(0), "Error_2", 
					"central_no = " + readyCentralList.get(0) + " , 資料日期 = " + new SimpleDateFormat("yyyyMMdd").format(record_date) + "  Migration Daily Run失敗!!");
			// 寫入runtime log
			ETL_P_Log.write_Runtime_Log("ETL_C_MigMaster", 
					"central_no = " + readyCentralList.get(0) + " , 資料日期 = " + new SimpleDateFormat("yyyyMMdd").format(record_date) + "  Migration Daily Run失敗!!");
			// 寫入執行失敗信件
			String mailContent = 
					"單位：" + readyCentralList.get(0) + "  資料日期：" + new SimpleDateFormat("yyyyMMdd").format(record_date) +
					"  執行DataMigration - 每日異動出現錯誤，請洽IT人員協助調查/處理。";
			ETL_Tool_Mail.writeAML_Mail(readyCentralList.get(0), "SYSAdmin", null, "AML_ETL 系統通知", mailContent);
		}
		
		try {
			// 若MigETL 資料日期為昨天以前最晚營業日, 則更新5代紀錄Table 讓備檔自動進行
			Date yesterdayRecordDate = ETL_C_Master.getBeforeRecordDate(new Date());
			System.out.println("yesterdayRecordDate = " + new SimpleDateFormat("yyyyMMdd").format(yesterdayRecordDate));
			if (record_date.equals(yesterdayRecordDate)) {
				Date nextRecordDate = ETL_C_Rerun.getNextRecordDate(yesterdayRecordDate);
				System.out.println("更新" + readyCentralList.get(0) + " "+ new SimpleDateFormat("yyyyMMdd").format(nextRecordDate) + " 備檔紀錄");
				ETL_C_FIVE_G.updateNewGenerationPrepareStatus(nextRecordDate, readyCentralList.get(0), null, "");
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("更新5代紀錄Table 讓備檔自動進行失敗!");
			ETL_P_Log.write_Runtime_Log("ETL_C_MigMaster", "更新5代紀錄Table 讓備檔自動進行失敗!");
		}
		
		// 更新ETL 執行紀錄  讓備檔作業可以繼續進行
		ETL_C_PROCESS.updateNewGenerationETLStatus(record_date, readyCentralList.get(0), "End", "");
		
		// 更新5代紀錄Table MigStatus 註記為結束
		ETL_C_PROCESS.updateNewGenerationMigStatus(record_date, readyCentralList.get(0), "End", "");
		
		// 清除交易性主檔180天前資料  // 第一次不開啟    2018.05.16  test  temp
//		if (remove_OLD_Datas(readyCentralList.get(0))) {
//			System.out.print(readyCentralList.get(0) + " 刪除交易性舊資料  成功!!");
//		} else {
//			System.out.print(readyCentralList.get(0) + " 刪除交易性舊資料  失敗!!");
//		}
		
		System.out.println("#### ETL_C_MigMaster End");
	}
	
	// 取得可用ETL Server資訊
	public static List<String[]> getUsableETLServer(String serverType, String usableStatus) {
		
		List<String[]> resultList = new ArrayList<String[]>();
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.getUsableETLServer(?,?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setString(2, serverType);
			cstmt.setString(3, usableStatus);
			cstmt.registerOutParameter(4, DB2Types.CURSOR);
			cstmt.registerOutParameter(5, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(5);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            ETL_P_Log.write_Runtime_Log("getUsableETLServer", "Error Code = " + returnCode + ", Error Message : " + errorMessage);
//	            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            
	            return resultList;
			}
			
			java.sql.ResultSet rs = (java.sql.ResultSet) cstmt.getObject(4);
			while (rs.next()) {
	        	String[] serverInto = new String[3];
	        	
	        	serverInto[0] = rs.getString(1); // Server No
	        	serverInto[1] = rs.getString(2); // Server Name
	        	serverInto[2] = rs.getString(3); // Server IP
	        	
	        	resultList.add(serverInto);
	        }
	        
//	        System.out.println("List Size = " + resultList.size()); // for test
		
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("getUsableETLServer", ex.getMessage());
		}
		
		return resultList;
	}
	
	// 檢核ETL Server 是否正常可連線
	// 排除連線異常，不可使用ETL Server，並給出提示訊息
	private static void filterETLServerOK(List<String[]> etlServerList) {
		
		for (int i = 0; i < etlServerList.size(); i++) {
			// 若ETL Server Service沒有正常啟動，該ETL Server會從List中移除
			if (!ETL_C_CallWS.checkETLServerStatus(etlServerList.get(i)[2])) {
				etlServerList.remove(i);
				i--;
			}
		}
	}
	
	// 取得待執行共用中心List
//	private static List<String> getMigETLCentral(Date record_date) {
//		
//		List<String> resultList = new ArrayList<String>();
//		
//		try {
//			
//			String sql = "{call " + ETL_Profile.db2TableSchema + ".Migration.getMigCentral(?,?,?,?)}";
//			
//			Connection con = ConnectionHelper.getDB2Connection();
//			CallableStatement cstmt = con.prepareCall(sql);
//			
//			cstmt.registerOutParameter(1, Types.INTEGER);
//			cstmt.setDate(2, new java.sql.Date(record_date.getTime()));
//			cstmt.registerOutParameter(3, DB2Types.CURSOR);
//			cstmt.registerOutParameter(4, Types.VARCHAR);
//			
//			cstmt.execute();
//			
//			int returnCode = cstmt.getInt(1);
//			
//			// 有錯誤釋出錯誤訊息   不往下繼續進行
//			if (returnCode != 0) {
//				String errorMessage = cstmt.getString(4);
//	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
////	            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
//	            ETL_P_Log.write_Runtime_Log("getNoProcessCentral", "Error Code = " + returnCode + ", Error Message : " + errorMessage);
//	            
//	            return resultList;
//			}
//			
//			java.sql.ResultSet rs = (java.sql.ResultSet) cstmt.getObject(3);
//			while (rs.next()) {
//	        	String centralInfo = rs.getString(1);
//	        	if (centralInfo != null) {
//	        		centralInfo = centralInfo.trim();
//	        	}
//	        	resultList.add(centralInfo);
//	        }
//	        
////	        System.out.println("List Size = " + resultList.size()); // for test
//		
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			ETL_P_Log.write_Runtime_Log("getNoProcessCentral", ex.getMessage());
//		}
//		
//		return resultList;
//	}
	
	// 取得批次編號(序號表ETL_PARAMETER_INFO)
	private static String getETL_BatchNo() {
		
		String result = "";
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.getETL_BatchNo(?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.registerOutParameter(2, Types.VARCHAR);
			cstmt.registerOutParameter(3, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(3);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
//	            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            ETL_P_Log.write_Runtime_Log("getETL_BatchNo", "Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            
	            return result;
			}
			
			result = cstmt.getString(2);
	        
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("getETL_BatchNo", ex.getMessage());
		}
		
		return result;
	}
	
	// 確認是否有對應Master檔
	private static boolean checkHasMasterTxt(String central_No) throws Exception {
		
		// 組成目標Master檔名
		String masterFileName = central_No + "MASTER.txt";
		String remoteFilePath = "/" + central_No + "/UPLOAD/";
		String remoteMasterFile = remoteFilePath + masterFileName;
		boolean hasMaster = ETL_SFTP.exist(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, 
				ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, remoteMasterFile);
		
		if (!hasMaster) {
			System.out.println("找不到" + remoteMasterFile + "檔案!");
			ETL_P_Log.write_Runtime_Log("checkHasMasterTxt", "找不到" + remoteMasterFile + "檔案!"); // for test
			return false;
		}
		
		return true;
	}
	
	// 取得就緒中心(檔案已上傳中心, 結合資料日期檢驗)
	private static List<String> checkReadyCentral(List<String> centralList, Date record_date, String batch_no) {
		List<String> resultLust = new ArrayList<String>();
		
		try {
			// 若檔案存在並檢查Master_Log是否處理過, 加入list表示就緒中心
			for (int i = 0; i < centralList.size(); i++) {
				if (checkHasMasterTxt(centralList.get(i))) {
					
					List<String> zipFiles = new ArrayList<String>();
					
					// 取得 List<資料日期|上傳批號 |zip檔名>
					zipFiles = parseMasterTxtContent(centralList.get(i));
					String[] dataInfo = zipFiles.get(0).split("\\|");
					
					// 確認是否為正確資料日期
					if (dataInfo[0].equals(new SimpleDateFormat("yyyyMMdd").format(record_date))) {
						System.out.println(centralList.get(i) + " Master 檔資料日期正確！");
						ETL_P_Log.write_Runtime_Log("checkReadyCentral", centralList.get(i) + " Master 檔資料日期正確！");
						
						// 確認是否為執行過資料(Master Log), 未執行過資料才列入待執行清單
						if (!hasMasterLog(record_date, centralList.get(i), dataInfo[1], batch_no)) {
							
							resultLust.add(centralList.get(i));
						} else {
							
							System.out.println(centralList.get(i) + " " + dataInfo[1] + "已經執行過！"); // for test
							ETL_P_Log.write_Runtime_Log("checkReadyCentral", centralList.get(i) + " " + dataInfo[1] + "已經執行過！");
							
							// 將已經執行過的Master txt刪除
							removeMasterTxt(centralList.get(i));
						}
						
					} else {
						System.out.println(centralList.get(i) + " Master 檔資料日期非預期:" + dataInfo[0]); // for test
					}
					
				}
				
			}
		} catch (Exception ex) {
			// 查詢檔案是否到期, 發生錯誤印出訊息
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("checkReadyCentral", ex.getMessage());
		}
			
		return resultLust;
	}
	
	// 解析Master檔內容
	private static List<String> parseMasterTxtContent(String central_No) throws Exception {
		
		// 結果字串
		List<String> resultList = new ArrayList<String>();
		
		String masterFileName = central_No + "MASTER.txt";
		String remoteFilePath = "/" + central_No + "/UPLOAD/";
		String remoteMasterFile = remoteFilePath + masterFileName;
		boolean hasMaster = ETL_SFTP.exist(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, 
				ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, remoteMasterFile);
		
		if (!hasMaster) {
			System.out.println("找不到" + remoteMasterFile + "檔案!");
			return null;
		}
		
		String localDownloadFilePath = ETL_C_Profile.ETL_Download_localPath + central_No + "/DOWNLOAD";
		File localDownloadFileDir = new File(localDownloadFilePath);
		if (!localDownloadFileDir.exists()) {
			localDownloadFileDir.mkdirs();
		}
		
		String localMasterFile = localDownloadFilePath 
				+ "/" + central_No + "MASTER_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".txt";
		
//			System.out.println(localMasterFile); // for test
//			System.out.println(remoteMasterFile); // for test
		
		// download Master檔
		if (ETL_SFTP.download(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, 
				ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, localMasterFile, remoteMasterFile)) {
			System.out.println("Download: " + remoteMasterFile + " 成功!");
		} else {
			System.out.println("Download: " + remoteMasterFile + " 失敗!");
			return null;
			// throw exception
		}
			
		// 讀取master檔內明細資料, 回傳zip檔list
		File parseFile = new File(localMasterFile);
		FileInputStream fis = new FileInputStream(parseFile);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis,"BIG5"));
		
		String masterLineStr = "";
		String resultStr = "";
		while (br.ready()) {
			masterLineStr = br.readLine();
			System.out.println(masterLineStr); // for test
			
			resultStr = checkMasterLineString(central_No, masterLineStr);
			if (resultStr == null) {
				throw new Exception("解析" + parseFile.getName() + "解析出現問題!");
			}
			
			resultList.add(resultStr);
		}
		
		br.close();
		fis.close();
		
		return resultList;
	}
	
	// 解析檢核Master File當中String
	private static String checkMasterLineString(String central_No, String input) {
		try {
			String[] strAry =  input.split("\\,");
			if (strAry.length != 2) {
				System.out.println("無法以,分隔"); // for test
				ETL_P_Log.write_Runtime_Log("checkMasterLineString", "無法以,分隔");
				return null;
			}
			
			// 檢核record_Date + upload_No
			if (strAry[0].length() != 11) {
				System.out.println("格式不正確, 前字長度不足。"); // for test
				ETL_P_Log.write_Runtime_Log("checkMasterLineString", "格式不正確, 前字長度不足。");
				return null;
			}
			
			// 資料日期
			String record_Date = strAry[0].substring(0, 8);
			// 上傳批號
			String upload_No = strAry[0].substring(8, 11);
			
			// 檢核日期格式
			if (!ETL_Tool_FormatCheck.checkDate(record_Date)) {
				return null;
			}
			
			// 檢核zip檔名
			String zipFileName = "AML_" + central_No + "_" + strAry[0] + ".zip";
			if (!zipFileName.equals(strAry[1])) {
				System.out.println("檔名檢核不通過:" + zipFileName + " - " + strAry[1]); // for test
				ETL_P_Log.write_Runtime_Log("checkMasterLineString", "檔名檢核不通過:" + zipFileName + " - " + strAry[1]);
				return null;
			}
			
			// 回傳  "(資料日期)|(上傳批號)|(zip檔名)"
			return record_Date + "|" + upload_No + "|" + zipFileName;
			
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("發生錯誤"); // for test
			ETL_P_Log.write_Runtime_Log("checkMasterLineString", ex.getMessage());
			return null;
		}
	}
	
	// 寫入Master Log
	private static boolean hasMasterLog(Date record_date, String central_No, String upload_No, String batch_no) {
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.query_Master_Log(?,?,?,?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(record_date.getTime()));
			cstmt.setString(3, central_No);
			cstmt.setString(4, upload_No);
			cstmt.setString(5, batch_no);
			cstmt.registerOutParameter(6, Types.VARCHAR);
			cstmt.registerOutParameter(7, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(7);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
//				throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            ETL_P_Log.write_Runtime_Log("hasMasterLog", "Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            
	            // 發生錯誤  回傳true 視為已經執行過，避免不必要的寫入
				return true;
			}
			
			String hasMaster = cstmt.getString(6);
			if ("Y".equals(hasMaster)) {
				return true;
			} else {
				return false;
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("hasMasterLog", ex.getMessage());
			// 發生錯誤  回傳true 視為已經執行過，避免不必要的寫入
			return true;
		}
	}
	
	// 刪除SFTP上Master.txt檔
	private static boolean removeMasterTxt(String central_no) {
		
		String masterFileName = central_no + "MASTER.txt";
		String remoteFilePath = "/" + central_no + "/UPLOAD/";
		String remoteMasterFile = remoteFilePath + masterFileName;
		
		if (ETL_SFTP.delete(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, 
				ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, remoteMasterFile)) {
			
			System.out.println("刪除 " + remoteMasterFile + " 成功！");
			ETL_P_Log.write_Runtime_Log("removeMasterTxt", "刪除 " + remoteMasterFile + " 成功！");
			return true;
		} else {
			
			System.out.println("刪除 " + remoteMasterFile + " 失敗！");
			ETL_P_Log.write_Runtime_Log("removeMasterTxt", "刪除 " + remoteMasterFile + " 失敗！");
			return false;
		}
		
	}
	
	// 寫入ETL完成紀錄
	private static boolean write_ETL_LOAD_GAML(String batch_no, Date record_date, String central_no, String upload_no) {
		
		try {
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.write_ETL_LOAD_GAML(?,?,?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setString(2, batch_no);
			cstmt.setDate(3, new java.sql.Date(record_date.getTime()));
			cstmt.setString(4, central_no);
			cstmt.setString(5, upload_no);
			cstmt.registerOutParameter(6, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(6);
	            System.out.println("####write_ETL_LOAD_GAML - Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            ETL_P_Log.write_Runtime_Log("write_ETL_LOAD_GAML", "####write_ETL_LOAD_GAML - Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            return false;
			}
			
			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("write_ETL_LOAD_GAML", ex.getMessage());
			return false;
		}
	}
	
	// 將中心src Table 進行runstate
//	private static void runStateSRC(String central_no) {
//		System.out.println("執行 runStateSRC " + central_no + " Start");
//		
//		try {
//			String sql = "{call " + ETL_Profile.db2TableSchema + ".Load.runStateSRC(?,?)}";
//			
//			Connection con = ConnectionHelper.getDB2Connection(central_no);
//			CallableStatement cstmt = con.prepareCall(sql);
//			
//			cstmt.registerOutParameter(1, Types.INTEGER);
//			cstmt.registerOutParameter(2, Types.VARCHAR);
//			
//			cstmt.execute();
//			
//			int returnCode = cstmt.getInt(1);
//			
//			// 有錯誤釋出錯誤訊息
//			if (returnCode != 0) {
//				String errorMessage = cstmt.getString(2);
//	            System.out.println("####runStateSRC - Error Code = " + returnCode + ", Error Message : " + errorMessage);
//	            ETL_P_Log.write_Runtime_Log("runStateSRC", "####runStateSRC - Error Code = " + returnCode + ", Error Message : " + errorMessage);
//			}
//			
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			ETL_P_Log.write_Runtime_Log("runStateSRC", ex.getMessage());
//		}
//		
//		System.out.println("執行 runStateSRC " + central_no + " End");
//	}
	
	// 清除交易性主檔180天前資料
	private static boolean remove_OLD_Datas(String central_no) {
		
		try {
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Load.remove_OLD_Datas(?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection(central_no);
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.registerOutParameter(2, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(2);
	            System.out.println("####remove_OLD_Datas - Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            ETL_P_Log.write_Runtime_Log("remove_OLD_Datas", "####remove_OLD_Datas - Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            return false;
			}
			
			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("remove_OLD_Datas", ex.getMessage());
			return false;
		}
	}
	
	// 查詢上一個資料日期
	private static Date getBeforeRecordDate(Date recordDate) throws Exception {
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.getBefore_RecordDate(?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(recordDate.getTime()));
			cstmt.registerOutParameter(3, Types.DATE);
			cstmt.registerOutParameter(4, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(4);
//	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
				ETL_P_Log.write_Runtime_Log("getBeforeRecordDate", "Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}
			
			return new java.util.Date(cstmt.getDate(3).getTime());
			
//	        System.out.println("List Size = " + resultList.size()); // for test
		
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("getBeforeRecordDate", ex.getMessage());
			throw new Exception("無法取得前一資料日期!");
		}
	}
	
	// 取得須進行Migration中心代號
	private static List<ETL_Bean_MigrationUnit> getMidMigrationList() {
		List<ETL_Bean_MigrationUnit> resultList = new ArrayList<ETL_Bean_MigrationUnit>();
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Migration.getMidMigrationCentral(?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.registerOutParameter(2, DB2Types.CURSOR);
			cstmt.registerOutParameter(3, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(3);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
//		            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            ETL_P_Log.write_Runtime_Log("getMigrationList", "Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            
	            return resultList;
			}
			
			java.sql.ResultSet rs = (java.sql.ResultSet) cstmt.getObject(2);
			while (rs.next()) {
	        	String centralInfo = rs.getString(1);
	        	Date recordDate = new Date(rs.getDate(2).getTime());
	        	if (centralInfo != null) {
	        		centralInfo = centralInfo.trim();
	        	}
	        	String batchNo = rs.getString(3);
	        	
	        	ETL_Bean_MigrationUnit data = new ETL_Bean_MigrationUnit();
	        	data.setCentralNo(centralInfo);
	        	data.setRecordDate(recordDate);
	        	data.setBatchNo(batchNo);
	        	
	        	resultList.add(data);
	        }
	        
//	        System.out.println("List Size = " + resultList.size()); // for test
		
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("getMigrationList", ex.getMessage());
		}
		
		return resultList;
	}
	
	// 確認rerun是否執行中
	private static boolean isRerunExecute() {
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.isRerunExecute(?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.registerOutParameter(2, Types.INTEGER);
			cstmt.registerOutParameter(3, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(3);
//	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
				ETL_P_Log.write_Runtime_Log("isRerunExecute", "Error Code = " + returnCode + ", Error Message : " + errorMessage);
		        return false;
			}
			
			int rerunCount = cstmt.getInt(2);
			
			// rerun資料批數
			if (rerunCount > 0) {
				return true;
			} else {
				return false;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("isRerunExecute", ex.getMessage());
			return false;
		}
	}
	
	public static void main(String[] args) {
		
		try {
		
			System.out.println("ETL_C_MigMaster 測試開始!");
			
//			boolean boo = hasMasterLog(new SimpleDateFormat("yyyyMMdd").parse("20180608"), "600", "001", "ETL00017");
//			System.out.println(boo);
//			System.out.println(hasMasterLog(new SimpleDateFormat("yyyyMMdd").parse("20180608"), "600", "001", "ETL00017"));
//			System.out.println(hasMasterLog(new SimpleDateFormat("yyyyMMdd").parse("20180608"), "600", "001", "ETL00018"));
//			System.out.println(hasMasterLog(new SimpleDateFormat("yyyyMMdd").parse("20180608"), "600", "002", "ETL00017"));
//			System.out.println(hasMasterLog(new SimpleDateFormat("yyyyMMdd").parse("20180608"), "600", "002", "MIG00013"));
//			System.out.println(hasMasterLog(new SimpleDateFormat("yyyyMMdd").parse("20180608"), "600", "002", "MIG00012"));
//			System.out.println(hasMasterLog(new SimpleDateFormat("yyyyMMdd").parse("20180313"), "910", "002", "ETL01001"));
//			System.out.println(hasMasterLog(new SimpleDateFormat("yyyyMMdd").parse("20180425"), "928", "003", "RER00028"));
//			System.out.println(hasMasterLog(new SimpleDateFormat("yyyyMMdd").parse("20180425"), "928", "002", "RER00028"));
			
			System.out.println("ETL_C_MigMaster 測試結束!");
			
	//		boolean add5GSuccess = false;
	//		try {
	//			add5GSuccess = addNew5G("ETL01034", ETL_Tool_StringX.toUtilDate("20180411"), "018", "001", "TEMP");
	//		} catch (Exception ex) {
	//			ex.printStackTrace();
	//		}
	//		runStateSRC("018");
	
			
	//		ETL_P_Log.write_Runtime_Log("Test", "Log writter");
			

//			
//			Date result = getBeforeRecordDate(new SimpleDateFormat("yyyyMMdd").parse("20180514"));
//			System.out.println(result);
//			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
}
