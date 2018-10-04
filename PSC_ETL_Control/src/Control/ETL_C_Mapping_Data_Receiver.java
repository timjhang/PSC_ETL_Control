package Control;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import Bean.ETL_Bean_LogData;
import Bean.ETL_Bean_Response;
import DB.ETL_P_Log;
import FTP.ETL_SFTP;
import Load.ETL_L_ERROR_LOG;
import Migration.ETL_DM_L;
import Migration.ETL_DM_MIGRATION_DAO;
import Tool.ETL_Tool_DM_ParseFileName;
import Tool.ETL_Tool_FileReader;
import Tool.ETL_Tool_StringX;

public class ETL_C_Mapping_Data_Receiver {

	private static String[] StartMappingStatusArr = {
			"Null",
			"End",
			"WAIT"
	};
	// 固定每分鐘開始執行程式
	public void execute() throws Exception {

		mappingDataReceiver("600");
		mappingDataReceiver("952");
		
		// mappingDataReceiver("018");
		// mappingDataReceiver("910");
		// mappingDataReceiver("928");
		// mappingDataReceiver("951");
		// mappingDataReceiver("605");
	}

	// 各單位對照檔起始點
	private void mappingDataReceiver(String central_no) throws Exception {

		boolean isSuccess = false;

		String directory = "/" + central_no + "/MIGRATION/";

		// 取得合乎規範的對照檔檔名
		List<String> mappingDataList;
		try {
			mappingDataList = ETL_Tool_FileReader.getTRTargetFileList(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, directory);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}

		// 確認對照檔檔名,數量是否正確
		if (!checkMappingDataFileName(central_no, mappingDataList)) {
			return;
		}

		// 取得可執行DMServer,並更改狀態
		String[] etlServer = getUsableETLServer();
		if (etlServer == null) {
			System.out.println("#### 無可用ETL SERVER 不進行作業");
			return;
		} else {
			System.out.println("#### 更改" + etlServer[0] + "為U ####");
			ETL_C_PROCESS.update_Server_Status(etlServer[0], "U");
		}
		try {
			System.out.println("#### 開始執行對照檔讀取 ####");

			// 確定要執行後 將要跑的對照檔移到run
			moveMappingDataToRun(central_no, mappingDataList);

			// 取得batch_no
			String batch_No = ETL_DM_MIGRATION_DAO.getMigration_BatchNo();
			// 解析對照檔
			ETL_Tool_DM_ParseFileName mappingData = new ETL_Tool_DM_ParseFileName(mappingDataList.get(0));

			// 檢查該段為狀態是否可重新執行對照檔 Null WAIT End
			if (chackMappingStatus(mappingData.getRecord_date(), central_no)) {

				System.out.println("#### 狀態正確 ####");

				// 押回狀態
				ETL_DM_MIGRATION_DAO.updateNewGenerationMigrationStatus(mappingData.getRecord_date(), central_no, "Null", batch_No);

				// 執行
				isSuccess = runMappingData(etlServer[2], central_no, batch_No, mappingData.getRecord_date_str(), etlServer[0]);

				// 執行成功 更新MigrationStatus
				if (isSuccess) {
					if (!ETL_C_Mapping_Data_Receiver.isTransCentralCodePoolSuccess(mappingData.getRecord_date(), central_no, "GAML" + central_no, batch_No)) {
						ETL_P_Log.write_Runtime_Log("DM", "runMappingData : isTransCentralCodePoolSuccess發生錯誤");
						System.out.println("####isTransCentralCodePoolSuccess 發生錯誤 ####");
						isSuccess = false;
					} else {
						// // 檢查是否是RERUN
						// if (isEndStatus(mappingData.getRecord_date(), central_no)) {
						// ETL_DM_MIGRATION_DAO.update_New_Generation_isRerun(mappingData.getRecord_date(),
						// central_no, 1);
						// }

						isSuccess = ETL_DM_MIGRATION_DAO.updateNewGenerationMigrationStatus(mappingData.getRecord_date(), central_no, "WAIT", batch_No);
					}
				}

			} else {
				System.out.println("狀態錯誤:該單位正在執行中");
				isSuccess = false;
			}

			// 正確會將資料移到 SFTP/600/MIGRATION/SUCCESS/TR_600_P_TRANSACTION_20180608.TXT (並刪除
			// SFTP/ERROR/TR_600_P_TRANSACTION_20180608) 並更新 TABLE狀態為WAIT
			// 執行中會將資料移到 SFTP/600/MIGRATION/RUNNING/TR_600_P_TRANSACTION_20180608.TXT (並刪除
			// SFTP/SUCCESS/TR_600_P_TRANSACTION_20180608)
			if (isSuccess) {
				String fromDirectory = "/" + central_no + "/MIGRATION/RUN/";
				String toDirectory = "/" + central_no + "/MIGRATION/SUCCESS/";
				ETL_SFTP.moveFile(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, fromDirectory, toDirectory, mappingDataList);
			} else {
				String fromDirectory = "/" + central_no + "/MIGRATION/RUN/";
				String toDirectory = "/" + central_no + "/MIGRATION/ERROR/";
				ETL_SFTP.moveFile(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, fromDirectory, toDirectory, mappingDataList);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			ETL_C_PROCESS.update_Server_Status(etlServer[0], "Y");
		}
	}

	// 將要跑的對照檔移到run
	private void moveMappingDataToRun(String central_no, List<String> list) {
		// 將可執行的資料移到run
		String fromDirectory = "/" + central_no + "/MIGRATION/";
		String toDirectory = "/" + central_no + "/MIGRATION/RUN/";

		ETL_SFTP.moveFile(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, fromDirectory, toDirectory, list);

		System.out.println("#### 將資料移動到run #### ");
	}

	// 抓到3個對照檔檢查格式是否正確(不管正確與否都會將該對照檔table 清空)
	private boolean runMappingData(String ip_port, String central_no, String batch_No, String record_DateStr, String server_No) {
		System.out.println("runMappingData :開始執行");
		boolean isSuccess = false;
		try {
			// 查詢有無機器可用 有可用就放該機器 IP
			// 如果該單位正在執行然後還在上傳對照檔.....暫時不檔
			String filePath = "/" + central_no + "/MIGRATION/RUN/";

			// 呼叫E讀取對照檔
			isSuccess = ETL_C_CallWS.call_ETL_Server_DMfunction(ip_port, filePath, batch_No, central_no, record_DateStr);

			if (isSuccess) {

				ETL_Bean_LogData logData = new ETL_Bean_LogData();
				logData.setBATCH_NO(batch_No);
				logData.setCENTRAL_NO(central_no);
				logData.setFILE_TYPE("");
				Date exc_record_date;

				exc_record_date = new SimpleDateFormat("yyyyMMdd").parse(record_DateStr);

				logData.setRECORD_DATE(exc_record_date);
				logData.setUPLOAD_NO("");

				String fedServer = "";
				// 設定fedName
				if ("ETL_S1".equals(server_No)) {
					fedServer = "ETLDB001";
				} else if ("ETL_S2".equals(server_No)) {
					fedServer = "ETLDB002";
				}

				ETL_L_ERROR_LOG obj = new ETL_L_ERROR_LOG(logData, fedServer, "RERUN");
				obj.trans_to_Error_Log(logData, fedServer, "RERUN");

				// 檢查對照檔是否成功 , 成功才拉資料
				if (ETL_DM_MIGRATION_DAO.checkMappingData(central_no, batch_No)) {
					
					logData.setPROGRAM_NO("ACCTMAPPING_LOAD");
					ETL_DM_L.trans_to_ACCTMAPPING_LOAD(logData, fedServer, "RERUN");

					logData.setPROGRAM_NO("BRANCHMAPPING_LOAD");
					ETL_DM_L.trans_to_BRANCHMAPPING_LOAD(logData, fedServer, "RERUN");
					
					logData.setPROGRAM_NO("IDMAPPING_LOAD");
					ETL_DM_L.trans_to_IDMAPPING_LOAD(logData, fedServer, "RERUN");
				} else {
					isSuccess = false;
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("runMappingData : 發生錯誤" + e.getMessage());
			ETL_P_Log.write_Runtime_Log("DM", "runMappingData : 發生錯誤" + e.getMessage());
			isSuccess = false;

			return isSuccess;
		}

		// 錯誤回傳false
		return isSuccess;
	}

	// 取得可用ETL SERVER 含驗證
	private String[] getUsableETLServer() {
		// 取得可用ETL SERVER
		List<String[]> etlServerList = ETL_C_Master.getUsableETLServer("ETL_SERVER", "Y");

		// 檢核ETL SERVER 是否正常可連線
		// 排除連線異常，不可使用ETL SERVER，並給出提示訊息
		ETL_C_Master.filterETLServerOK(etlServerList);

		if (etlServerList.size() == 0) {
			System.out.println("#### 無可用ETL SERVER 不進行作業");
			ETL_P_Log.write_Runtime_Log("DM", "#### 無可用ETL SERVER 不進行作業");
		}

		// 呼叫確認用Web Service連線
		System.out.println("etlServerList size = " + etlServerList.size()); // for test
		System.out.println("Usable ETL SERVER List :");
		for (int i = 0; i < etlServerList.size(); i++) {
			System.out.println("Server_No : " + etlServerList.get(i)[0] + " , Server : " + etlServerList.get(i)[1] + " , IP : " + etlServerList.get(i)[2]);
		}

		if (etlServerList.size() == 0) {
			return null;
		} else {
			return etlServerList.get(0);
		}
	}

	// 確認對照檔檔名,數量
	private boolean checkMappingDataFileName(String central_no, List<String> list) {
		List<ETL_Tool_DM_ParseFileName> parseFileNames = new ArrayList<ETL_Tool_DM_ParseFileName>();
		String fromDirectory = "/" + central_no + "/MIGRATION/";
		String toDirectory = "/" + central_no + "/MIGRATION/ERROR/";

		try {
			// 檢查對照檔數量 一個單位只能處理1個 dm 所以對照檔只能是三個
			if (list.size() > 3) {
				ETL_SFTP.moveFile(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, fromDirectory, toDirectory, list);
				System.out.println("數量不合要求:" + list.size());
				return false;
				
			//資料未到齊
			} else if (list.size() < 3) {
				return false;
			}

			
			for (String mappingData : list) {
				parseFileNames.add(new ETL_Tool_DM_ParseFileName(mappingData));
			}
			ETL_Tool_DM_ParseFileName mappingData1 = null;
			
			Long chackLong = null;
			String chacktFile_name = null;
			for(ETL_Tool_DM_ParseFileName parseFileName:parseFileNames) {
				//第一次
				if (chackLong == null && chacktFile_name == null) {
					chackLong =parseFileName.getRecord_date().getTime();
					chacktFile_name = parseFileName.getFile_name();
					
				// 檢查對照檔日期是否有一致
				}else if(chackLong!=parseFileName.getRecord_date().getTime()){
					ETL_SFTP.moveFile(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, fromDirectory, toDirectory, list);
					System.out.println("對照檔日期不一致:");//
					System.out.println("#### 檢核失敗不進行作業 ####");
					return false;
				
				// 檢查對照檔檔名類型 不可為相同
				}else if(chacktFile_name==parseFileName.getFile_name()){
					ETL_SFTP.moveFile(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, fromDirectory, toDirectory, list);
					System.out.println("對照檔檔名相同:");
					System.out.println("#### 檢核失敗不進行作業 ####");
					return false;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();

			ETL_SFTP.moveFile(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, fromDirectory, toDirectory, list);

			return false;
		}

		System.out.println("對照檔檔名檢核通過");
		for (String str : list) {
			System.out.println("對照檔檔名:" + str);
		}

		return true;
	}

	private static boolean isTransCentralCodePoolSuccess(Date record_date, String central_no, String fed,String batch_No) {
		
		System.out.println("####trans_CentralCodePool Start ####");
		
		if (!ETL_DM_MIGRATION_DAO.trans_CentralCodePool(record_date, "GAML" + central_no, central_no, batch_No)) {
			ETL_P_Log.write_Runtime_Log("DM", "runMappingData : trans_CentralCodePool發生錯誤");
			System.out.println("####trans_CentralCodePool 發生錯誤 ####");
			return false;
		}

		// 同步CentralCodePool
		if (!ETL_DM_MIGRATION_DAO.synchronize_CentralCodePool(record_date, fed, "001")) {
			ETL_P_Log.write_Runtime_Log("DM", "runMappingData : synchronize_CentralCodePool ETLDB001發生錯誤");
			System.out.println("####trans_CentralCodePool 發生錯誤 ####");

		}

		if (!ETL_DM_MIGRATION_DAO.synchronize_CentralCodePool(record_date, fed, "002")) {
			ETL_P_Log.write_Runtime_Log("DM", "runMappingData : synchronize_CentralCodePool ETLDB002發生錯誤");
			System.out.println("####trans_CentralCodePool 發生錯誤 ####");
		}

		System.out.println("####trans_CentralCodePool END ####");
		
		return true;

	}
	
	private static boolean chackMappingStatus(Date record_date, String central_no) {
		ETL_Bean_Response res = ETL_DM_MIGRATION_DAO.get_Migration_Status(record_date, central_no);

		if (!res.isSuccess()) {
			System.out.println(res.getError());
			return false;
		} else {
			String resStatus = res.getObj().toString();
			System.out.println("目前狀態為: "+resStatus);
			for (String status : StartMappingStatusArr) {
				if (status.equals(resStatus)) {
					return true;
				}
			}
		}
		return false;
	}

	public static void main(String[] args) throws Exception {
		
		System.out.println(chackMappingStatus(new SimpleDateFormat("yyyyMMdd").parse("20190713"), "600"));

//		// ETL_DM_MIGRATION_DAO.updateNewGenerationMigrationStatus(new
//		// SimpleDateFormat("yyyyMMdd").parse("20180625"),
//
//		if (!ETL_DM_MIGRATION_DAO.synchronize_CentralCodePool(ETL_Tool_StringX.toUtilDate("2080608"), "GAML600", "002")) {
//			ETL_P_Log.write_Runtime_Log("DM", "runMappingData : synchronize_CentralCodePool ETLDB002發生錯誤");
//			System.out.println("####trans_CentralCodePool 發生錯誤 ####");
//		} else {
//			System.out.println("####trans_CentralCodePool 正確 ####");
//		}
//
//		// String toDirectory ="/"+"951/MIGRATION/RUN/";
//		// String fromDirectory ="/"+"951"+"/MIGRATION/";
//		//
//		// List<String> lists = new ArrayList<String>();
//		// lists.add("TEST.txt");
//		// lists.add("123.txt");
//		//
//		//
//		// System.out.println(ETL_Tool_DES.decrypt("de6964a598384312250db3230e803611"));
//		// ETL_SFTP.moveFile(ETL_C_Profile.sftp_hostName,Integer.valueOf(ETL_C_Profile.sftp_port),ETL_C_Profile.sftp_username,
//		// ETL_C_Profile.sftp_password,fromDirectory, toDirectory,lists);
//
//		// String localFilePath =
//		// "C:/Users/10404003/Desktop/temp/Sftp_Download/test4.txt";

	}

}
