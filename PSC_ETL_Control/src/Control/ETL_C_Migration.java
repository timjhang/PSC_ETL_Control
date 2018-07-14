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
import DB.ConnectionHelper;
import DB.ETL_P_Log;
import FTP.ETL_SFTP;
import Profile.ETL_Profile;
import Tool.ETL_Tool_FormatCheck;

public class ETL_C_Migration {
	
	// 是否為正式參數
	private static boolean isFormal = false;

	// 固定每日00:01開始執行程式
	public static void execute() {
		
		Calendar c1 = Calendar.getInstance();
    	final String strTime = String.format("%1$tH%1$tM", c1);
		
    	//  使用編號" 1"設定檔(BatchRunTimeConfig)
    	boolean isRun = false;
    	String runBatchNo = "";
    	
//    	if (isFormal) {
    		// 正式版本
    		runBatchNo = " 9";
    		isRun = ETL_C_BatchTime.isExecute(strTime, runBatchNo);
//    	} else {
//    		// 測試版本
//    		runBatchNo = " 4";
//    		isRun = ETL_C_BatchTime.isExecute(strTime, runBatchNo); // for  test
//    	}
    	
		if (!isRun) {
			System.out.println("isRun = false  " + runBatchNo);
			return;
		}
		
		// 若Rerun執行, 則Migration執行等待
    	if (ETL_C_Master.isRerunExecute()) {
    		System.out.println("####ETL_C_Migration - Rerun 作業進行, 不進行Migration作業。  " + runBatchNo);
    		ETL_P_Log.write_Runtime_Log("ETL_C_Migration", "####ETL_C_Migration - Rerun 作業進行, 不進行Migration作業。  " + runBatchNo);
    		return;
    	}
    	
    	// 若ETL正常執行, 則Migration等待
    	// test temp
    	if (false) {
    		System.out.println("####ETL_C_Migration - ETL 作業進行中, 不進行Migration作業。  " + runBatchNo);
    		ETL_P_Log.write_Runtime_Log("ETL_C_Migration", "####ETL_C_Migration - ETL 作業進行中, 不進行Migration作業。  " + runBatchNo);
    		return;
    	}
    	
    	// 若單位備檔進行中, 則Migration等待
    	if (false) {
    		System.out.println("####ETL_C_Migration - 備檔作業進行中, 不進行Migration作業。  " + runBatchNo);
    		ETL_P_Log.write_Runtime_Log("ETL_C_Migration", "####ETL_C_Migration - 備檔作業進行中, 不進行Migration作業。  " + runBatchNo);
    		return;
    	}
		
		System.out.println("#### ETL_C_Migration Start  " + runBatchNo);
		
		// *執行rerun時, 寫入一筆紀錄, 將wait到rerun結束後執行
		
		/* 取得可用ETL_Server資訊(資訊表ETL_SERVER_INFO), 檢查過濾ETL_Server是否正常可用 */
		// 如果具有可用ETL Server, 才開始進行, 否則這一輪結束
		
		// 取得可用ETL Server
		List<String[]> etlServerList = ETL_C_Master.getUsableETLServer("ETL_SERVER", "Y");
		
		// 檢核ETL Server 是否正常可連線
		// 排除連線異常，不可使用ETL Server，並給出提示訊息
		ETL_C_Master.filterETLServerOK(etlServerList);
		
		if (etlServerList.size() == 0) {
			System.out.println("#### 無可用ETL Server 不進行作業");
			ETL_P_Log.write_Runtime_Log("ETL_C_Migration", "#### 無可用ETL Server 不進行作業");
			return;
		}
		
		// 呼叫確認用Web Service連線
 		System.out.println("etlServerList size = " + etlServerList.size()); // for test
		System.out.println("Usable ETL Server List :");
		for (int i = 0; i < etlServerList.size(); i++) {
			System.out.println("Server_No : " + etlServerList.get(i)[0] + " , Server : " + etlServerList.get(i)[1] + " , IP : " + etlServerList.get(i)[2]);
		}
		
		// 取得執行中心代號(序號表ETL_CENTRAL_INFO), 取得今天未執行中心
		List<ETL_Bean_MigrationUnit> needMigCentralList = getMigrationList();
		
		System.out.println("needMigCentralList size = " + needMigCentralList.size()); // for test
		ETL_P_Log.write_Runtime_Log("ETL_C_Migration", "noProcessCentralList size = " + needMigCentralList.size());
		
		if (needMigCentralList.size() == 0) {
			System.out.println("#### needMigCentralList 無需要處理中心");
			ETL_P_Log.write_Runtime_Log("ETL_C_Migration", "#### needMigCentralList 無需要處理中心");
			return;
		}
		System.out.println("need Migration Central List :");
		for (int i = 0; i < needMigCentralList.size(); i++) {
			System.out.println(needMigCentralList.get(i).getCentralNo() + " " + new SimpleDateFormat("yyyyMMdd").format(needMigCentralList.get(i).getRecordDate()));
		}
		
		// 取得Migration Date
		Date mig_record_date = needMigCentralList.get(0).getRecordDate();
		Date beforeRecordDate;
		
		try {
			beforeRecordDate = ETL_C_Master.getBeforeRecordDate(mig_record_date);
        } catch (Exception ex) {
        	ex.printStackTrace();
        	System.out.println("無法取得最近資料日期，無法繼續進行！");
        	ETL_P_Log.write_Runtime_Log("ETL_C_Migration", "無法取得上一個資料日期，無法繼續進行！");
        	return;
        }
		
		System.out.println("mig_record_date = " + mig_record_date); // for test
		
		// 掃描中心SFTP Migration檔案是否已上傳
		List<ETL_Bean_MigrationUnit> readyCentralList = checkMigrationReadyCentral(needMigCentralList, mig_record_date);
		System.out.println("readyMigrationCentralList size = " + readyCentralList.size()); // for test
		ETL_P_Log.write_Runtime_Log("ETL_C_Migration", "readyMigrationCentralList size = " + readyCentralList.size());
		
		if (readyCentralList.size() == 0) {
			System.out.println("#### readyMigrationCentralList 無需要處理中心");
			ETL_P_Log.write_Runtime_Log("ETL_C_Migration", "#### readyMigrationCentralList 無需要處理中心");
			return;
		}
		System.out.println("ReadyMigrationCentralList :");
		ETL_P_Log.write_Runtime_Log("ETL_C_Migration", "ReadyCentralList :");
		for (int i = 0; i < readyCentralList.size(); i++) {
			System.out.println(readyCentralList.get(i).getCentralNo());
			ETL_P_Log.write_Runtime_Log("ETL_C_Migration", readyCentralList.get(i).getCentralNo());
		}
		
		// 取得批次編號(序號表ETL_PARAMETER_INFO)
		String batchNo = getMigration_BatchNo();
		System.out.println("MigBatchNo = " + batchNo); // for test
		ETL_P_Log.write_Runtime_Log("ETL_C_Migration", "MigBatchNo = " + batchNo);
		
		String[] ptr_upload_no = new String[1];
		
		// 更新 新5代製作紀錄檔 - Migration  註冊  Start
		ETL_C_PROCESS.updateNewGenerationMigStatus(mig_record_date, readyCentralList.get(0).getCentralNo(), "Start", "");
		
		// 清除可能留下load rerun Table
		if (!ETL_C_New5G.clearLoadTable(readyCentralList.get(0).getCentralNo(), "RERUN")) {
			System.out.println("清除相關Rerun Table 發生錯誤!");
			ETL_P_Log.write_Runtime_Log("ETL_C_Migration", "清除相關Rerun Table 發生錯誤!");
			return;
		}
		
		// Transaction超過一定門檻時執行reorg(產生新一代後即可能進行ETL作業, 故先進行可能的reorg)
		ETL_C_FIVE_G.reorgTransaction(readyCentralList.get(0).getCentralNo(), beforeRecordDate);
		
		// 建立新一代load rerun Table
		if (!ETL_C_FIVE_G.generateNewGTable(beforeRecordDate, mig_record_date, readyCentralList.get(0).getCentralNo(), "RERUN")) {
			System.out.println("####ETL_C_Rerun - 建立 " + readyCentralList.get(0).getCentralNo() + " 新一代Rerun Table 資料日期" 
					+ new SimpleDateFormat("yyyy-MM-dd").format(mig_record_date) + "發生錯誤！");
		}
			
		// 執行Migration異動
		boolean etlSuccess = ETL_C_PROCESS.executeMigration(etlServerList.get(0), batchNo, readyCentralList.get(0).getCentralNo(), ptr_upload_no, mig_record_date, beforeRecordDate);
		
		if (etlSuccess) {
			System.out.println("central_no = " + readyCentralList.get(0).getCentralNo() + " , 資料日期 = " + new SimpleDateFormat("yyyyMMdd").format(mig_record_date) + "  Migration成功!!");
		} else {
			System.out.println("central_no = " + readyCentralList.get(0).getCentralNo() + " , 資料日期 = " + new SimpleDateFormat("yyyyMMdd").format(mig_record_date) + "  Migration失敗!!");
			ETL_P_Log.write_Runtime_Log("ETL_C_Migration", 
					"central_no = " + readyCentralList.get(0).getCentralNo() + " , 資料日期 = " + new SimpleDateFormat("yyyyMMdd").format(mig_record_date) + "  Migration失敗!!");
			return;
		}
		
		// 接著繼續進行異動資料處理, 重新取得批次編號
		batchNo = ETL_C_Rerun.getRerun_BatchNo();
		
		// 執行Daily異動
		etlSuccess = ETL_C_PROCESS.executeMigETL(etlServerList.get(0), batchNo, readyCentralList.get(0).getCentralNo(), ptr_upload_no, mig_record_date, beforeRecordDate);
		
		// 執行正常完整, 則寫入
		if (etlSuccess) {
			
			if (isFormal) {
				// 執行runstate程式
	    		ETL_C_Master.runStateSRC(readyCentralList.get(0).getCentralNo());
			} else {
//				// 執行runstate程式
//	    		ETL_C_Master.runStateSRC(readyCentralList.get(0).getCentralNo());
			}
			
			if (isFormal) {
				// 寫入ETL完成紀錄ETL_LOAD_GAML
				boolean isSuccess = ETL_C_Master.write_ETL_LOAD_GAML(batchNo, mig_record_date, readyCentralList.get(0).getCentralNo(), ptr_upload_no[0]);
				
				System.out.println("batchNo = " + batchNo + " , record_date = " + mig_record_date 
						+ " , central_no = " + readyCentralList.get(0).getCentralNo() + " , upload_no = " + ptr_upload_no[0]);
				if (isSuccess) {
					System.out.println("寫入ETL_LOAD_GAML成功!");
					ETL_P_Log.write_Runtime_Log("ETL_C_Migration", "寫入ETL_LOAD_GAML成功!");
				} else {
					System.out.println("寫入ETL_LOAD_GAML失敗!");
					ETL_P_Log.write_Runtime_Log("ETL_C_Migration", "寫入ETL_LOAD_GAML失敗!");
				}
			} else {
//				// 寫入ETL完成紀錄ETL_LOAD_GAML
//				boolean isSuccess = ETL_C_Master.write_ETL_LOAD_GAML(batchNo, mig_record_date, readyCentralList.get(0).getCentralNo(), ptr_upload_no[0]);
//				
//				System.out.println("batchNo = " + batchNo + " , record_date = " + mig_record_date 
//						+ " , central_no = " + readyCentralList.get(0).getCentralNo() + " , upload_no = " + ptr_upload_no[0]);
//				if (isSuccess) {
//					System.out.println("寫入ETL_LOAD_GAML成功!");
//					ETL_P_Log.write_Runtime_Log("ETL_C_Migration", "寫入ETL_LOAD_GAML成功!");
//				} else {
//					System.out.println("寫入ETL_LOAD_GAML失敗!");
//					ETL_P_Log.write_Runtime_Log("ETL_C_Migration", "寫入ETL_LOAD_GAML失敗!");
//				}
			}
			
			// 更新 新5代製作紀錄檔 - Migration  註冊  End
			ETL_C_PROCESS.updateNewGenerationMigStatus(mig_record_date, readyCentralList.get(0).getCentralNo(), "End", "");
		}
		
		// 執行完成更新Flag, 使後續運作正常
//		123
		
		System.out.println("#### ETL_C_Migration End");
	}
	
	// 取得Migration批次編號(序號表ETL_PARAMETER_INFO)
	private static String getMigration_BatchNo() {
		
		String result = "";
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Migration.getMigration_BatchNo(?,?,?)}";
			
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
//			            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            ETL_P_Log.write_Runtime_Log("getMigration_BatchNo", "Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            
	            return result;
			}
			
			result = cstmt.getString(2);
	        
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("getMigration_BatchNo", ex.getMessage());
		}
		
		return result;
	}
	
	// 取得就緒中心(檔案已上傳中心, 結合資料日期檢驗)
	private static List<ETL_Bean_MigrationUnit> checkMigrationReadyCentral(List<ETL_Bean_MigrationUnit> centralList, Date record_date) {
		List<ETL_Bean_MigrationUnit> resultLust = new ArrayList<ETL_Bean_MigrationUnit>();
		
		try {
			// 若檔案存在並檢查Master_Log是否處理過, 加入list表示就緒中心
			for (int i = 0; i < centralList.size(); i++) {
				if (checkHasMigrationMasterTxt(centralList.get(i).getCentralNo())) {
					
					List<String> zipFiles = new ArrayList<String>();
					
					// 取得 List<資料日期|上傳批號 |zip檔名>
					zipFiles = parseMigrationMasterTxtContent(centralList.get(i).getCentralNo());
					String[] dataInfo = zipFiles.get(0).split("\\|");
					
					// 確認是否為正確資料日期
					if (dataInfo[0].equals(new SimpleDateFormat("yyyyMMdd").format(record_date))) {
						System.out.println(centralList.get(i).getCentralNo() + " Master 檔資料日期正確！");
						ETL_P_Log.write_Runtime_Log("checkMigrationReadyCentral", centralList.get(i).getCentralNo() + " Master 檔資料日期正確！");
						
						resultLust.add(centralList.get(i));
						
					} else {
						System.out.println(centralList.get(i).getCentralNo() + " Master 檔資料日期非預期:" + dataInfo[0]); // for test
					}
					
				}
				
			}
		} catch (Exception ex) {
			// 查詢檔案是否到期, 發生錯誤印出訊息
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("checkMigrationReadyCentral", ex.getMessage());
		}
			
		return resultLust;
	}
	
	// 確認是否有對應Migration Master檔
	private static boolean checkHasMigrationMasterTxt(String central_No) throws Exception {
		
		// 組成目標Master檔名
		String masterFileName = "TR_" + central_No + "MASTER.txt";
		String remoteFilePath = "/" + central_No + "/MIGRATION/";
		String remoteMasterFile = remoteFilePath + masterFileName;
		boolean hasMaster = ETL_SFTP.exist(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, 
				ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, remoteMasterFile);
		
		if (!hasMaster) {
			System.out.println("找不到" + remoteMasterFile + "檔案!");
			ETL_P_Log.write_Runtime_Log("checkHasMigrationMasterTxt", "找不到" + remoteMasterFile + "檔案!"); // for test
			return false;
		}
		
		return true;
	}
	
	// 解析Migration Master檔內容
	private static List<String> parseMigrationMasterTxtContent(String central_No) throws Exception {
		
		// 結果字串
		List<String> resultList = new ArrayList<String>();
		
		String masterFileName = "TR_" + central_No + "MASTER.txt";
		String remoteFilePath = "/" + central_No + "/MIGRATION/";
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
				+ "/TR_" + central_No + "MASTER_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".txt";
		
//				System.out.println(localMasterFile); // for test
//				System.out.println(remoteMasterFile); // for test
		
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
			
			resultStr = checkMigrationMasterLineString(central_No, masterLineStr);
			if (resultStr == null) {
				throw new Exception("解析" + parseFile.getName() + "解析出現問題!");
			}
			
			resultList.add(resultStr);
		}
		
		br.close();
		fis.close();
		
		return resultList;
	}
	
	// 解析檢核Migration Master File當中String
	private static String checkMigrationMasterLineString(String central_No, String input) {
		try {
			String[] strAry =  input.split("\\,");
			if (strAry.length != 2) {
				System.out.println("無法以,分隔"); // for test
				ETL_P_Log.write_Runtime_Log("checkMigrationMasterLineString", "無法以,分隔");
				return null;
			}
			
			// 檢核record_Date + upload_No
			if (strAry[0].length() != 11) {
				System.out.println("格式不正確, 前字長度不足。"); // for test
				ETL_P_Log.write_Runtime_Log("checkMigrationMasterLineString", "格式不正確, 前字長度不足。");
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
			String zipFileName = "AML_TR_" + central_No + "_" + strAry[0] + ".zip";
			if (!zipFileName.equals(strAry[1])) {
				System.out.println("檔名檢核不通過:" + zipFileName + " - " + strAry[1]); // for test
				ETL_P_Log.write_Runtime_Log("checkMigrationMasterLineString", "檔名檢核不通過:" + zipFileName + " - " + strAry[1]);
				return null;
			}
			
			// 回傳  "(資料日期)|(上傳批號)|(zip檔名)"
			return record_Date + "|" + upload_No + "|" + zipFileName;
			
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("發生錯誤"); // for test
			ETL_P_Log.write_Runtime_Log("checkMigrationMasterLineString", ex.getMessage());
			return null;
		}
	}
	
	// 取得須進行Migration中心代號
	private static List<ETL_Bean_MigrationUnit> getMigrationList() {
		List<ETL_Bean_MigrationUnit> resultList = new ArrayList<ETL_Bean_MigrationUnit>();
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Migration.getMigrationCentral(?,?,?)}";
			
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
//	            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
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
	        	
	        	ETL_Bean_MigrationUnit data = new ETL_Bean_MigrationUnit();
	        	data.setCentralNo(centralInfo);
	        	data.setRecordDate(recordDate);
	        	
	        	resultList.add(data);
	        }
	        
	        System.out.println("List Size = " + resultList.size()); // for test
		
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("getMigrationList", ex.getMessage());
		}
		
		return resultList;
	}
	
	public static void main(String[] args) {
		
		System.out.println("ETL_C_Migration 測試開始!");
		
		List<ETL_Bean_MigrationUnit> list = getMigrationList();
		
		for (int i = 0; i < list.size(); i++) {
			System.out.println(list.get(i).getCentralNo() + " " +
					new SimpleDateFormat("yyyyMMdd").format(list.get(i).getRecordDate()));
		}
		
		System.out.println("ETL_C_Migration 測試結束!");
		
	}

}
