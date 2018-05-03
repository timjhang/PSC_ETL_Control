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

import Control.ETL_C_Profile;
import DB.ConnectionHelper;
import FTP.ETL_SFTP;
import Profile.ETL_Profile;
import Tool.ETL_Tool_FormatCheck;

public class ETL_C_Master {

	// 固定每日00:01開始執行程式
	public static void execute() {
		
		Calendar c1 = Calendar.getInstance();
    	final String strTime = String.format("%1$tH%1$tM", c1);
		
    	//  使用編號" 1"設定檔(BatchRunTimeConfig)
		boolean isRun = ETL_C_BatchTime.isExecute(strTime, " 1");
		if (!isRun) {
			
			System.out.println("isRun = false");
			return;
		}
		
		System.out.println("#### ETL_C_Master Start");
		
		// *執行rerun時, 寫入一筆紀錄, 將wait到rerun結束後執行
		
		/* 取得可用ETL_Server資訊(資訊表ETL_SERVER_INFO), 檢查過濾ETL_Server是否正常可用 */
		// 如果具有可用ETL Server, 才開始進行, 否則這一輪結束
		
		// 取得可用ETL Server
		List<String[]> etlServerList = getUsableETLServer("ETL_SERVER", "Y");
		
		// 檢核ETL Server 是否正常可連線
		// 排除連線異常，不可使用ETL Server，並給出提示訊息
		filterETLServerOK(etlServerList);
		
		if (etlServerList.size() == 0) {
			System.out.println("#### 無可用ETL Server 不進行作業");
			return;
		}
		
		// 呼叫確認用Web Service連線
 		System.out.println("etlServerList size = " + etlServerList.size()); // for test
		System.out.println("Usable ETL Server List :");
		for (int i = 0; i < etlServerList.size(); i++) {
			System.out.println("Server_No : " + etlServerList.get(i)[0] + " , Server : " + etlServerList.get(i)[1] + " , IP : " + etlServerList.get(i)[2]);
		}
		
		// 產生資料日期(昨天)
//		Calendar cal = Calendar.getInstance(); // 今天時間
//        cal.add(Calendar.DATE, -1); // 昨天時間
//        Date record_date = cal.getTime();
		
		// for test
		Date record_date;
		Date before_record_date;
		try {
			before_record_date = new SimpleDateFormat("yyyyMMdd").parse("20180426");
			record_date = new SimpleDateFormat("yyyyMMdd").parse("20180427");
		} catch (Exception ex) {
			ex.printStackTrace();
			before_record_date = new Date();
			record_date = new Date();
		}
		
        System.out.println(record_date); // for test
		
		// 取得執行中心代號(序號表ETL_CENTRAL_INFO), 取得今天未執行中心
		List<String> noProcessCentralList = getNoProcessCentral(record_date);
		System.out.println("noProcessCentralList size = " + noProcessCentralList.size()); // for test
		if (noProcessCentralList.size() == 0) {
			System.out.println("#### 無需要處理中心");
			return;
		}
		System.out.println("no Process Central List :");
		for (int i = 0; i < noProcessCentralList.size(); i++) {
			System.out.println(noProcessCentralList.get(i));
		}
		
		// 取得批次編號(序號表ETL_PARAMETER_INFO)
		String batchNo = getETL_BatchNo();
		System.out.println("BatchNo = " + batchNo); // for test
		
		// 掃描中心SFTP今日檔案是否已上傳
//		List<String> readyCentralList = checkReadyCentral(noProcessCentralList);
		List<String> readyCentralList = checkReadyCentral(noProcessCentralList, record_date);
		System.out.println("readyCentralList size = " + readyCentralList.size()); // for test
		System.out.println("ReadyCentralList :");
		for (int i = 0; i < readyCentralList.size(); i++) {
			System.out.println(readyCentralList.get(i));
		}
		
		// 指定ETL任務
		ETL_C_PROCESS.executeETL(etlServerList.get(0), batchNo, readyCentralList.get(0), record_date, before_record_date);
		
		System.out.println("#### ETL_C_Master End");
	}
	
	// 取得可用ETL Server資訊
	private static List<String[]> getUsableETLServer(String serverType, String usableStatus) {
		
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
	private static List<String> getNoProcessCentral(Date record_date) {
		
		List<String> resultList = new ArrayList<String>();
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.getNoProcessCentral(?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(record_date.getTime()));
			cstmt.registerOutParameter(3, DB2Types.CURSOR);
			cstmt.registerOutParameter(4, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(4);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
//	            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            
	            return resultList;
			}
			
			java.sql.ResultSet rs = (java.sql.ResultSet) cstmt.getObject(3);
			while (rs.next()) {
	        	String centralInfo = rs.getString(1);
	        	if (centralInfo != null) {
	        		centralInfo = centralInfo.trim();
	        	}
	        	resultList.add(centralInfo);
	        }
	        
//	        System.out.println("List Size = " + resultList.size()); // for test
		
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		return resultList;
	}
	
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
	            
	            return result;
			}
			
			result = cstmt.getString(2);
	        
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		return result;
	}
	
	// 取得就緒中心(檔案已上傳中心)
//	private static List<String> checkReadyCentral(List<String> centralList) {
//		List<String> resultLust = new ArrayList<String>();
//		
//		try {
//			// 若檔案存在並檢查Master_Log是否處理過, 加入list表示就緒中心
//			for (int i = 0; i < centralList.size(); i++) {
//				if (checkHasMaster(centralList.get(i))) {
//					resultLust.add(centralList.get(i));
//				}
//			}
//		} catch (Exception ex) {
//			// 查詢檔案是否到期, 發生錯誤印出訊息
//			ex.printStackTrace();
//			// 寫入log ????
//		}
//			
//		return resultLust;
//	}
	
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
			return false;
		}
		
		return true;
	}
	
	// 取得就緒中心(檔案已上傳中心, 結合資料日期檢驗)
	private static List<String> checkReadyCentral(List<String> centralList, Date record_date) {
		List<String> resultLust = new ArrayList<String>();
		
		try {
			// 若檔案存在並檢查Master_Log是否處理過, 加入list表示就緒中心
			for (int i = 0; i < centralList.size(); i++) {
				if (checkHasMasterTxt(centralList.get(i))) {
					
					List<String> zipFiles = new ArrayList<String>();
					
					// 非rerun版本
					// 取得 List<資料日期|上傳批號 |zip檔名>
					zipFiles = parseMasterTxtContent(centralList.get(i));
					String[] dataInfo = zipFiles.get(0).split("\\|");
					
					// 確認是否為正確資料日期
					if (dataInfo[0].equals(new SimpleDateFormat("yyyyMMdd").format(record_date))) {
						System.out.println(centralList.get(i) + " Master 檔資料日期正確！");
						
						// 確認是否為執行過資料(Master Log), 未執行過資料才列入待執行清單
						if (!hasMasterLog(record_date, centralList.get(i), dataInfo[1])) {
							
							resultLust.add(centralList.get(i));
						} else {
							
							System.out.println(centralList.get(i) + " " + dataInfo[1] + "已經執行過！"); // for test
							
							// 將已經執行過的Master txt刪除
							removeMasterTxt(centralList.get(i));
						}
						
					} else {
						System.out.println(centralList.get(i) + " Master 檔資料日期非預期:" + dataInfo[0]); // for test
					}
					
					// rerun版本 ??????
				}
				
			}
		} catch (Exception ex) {
			// 查詢檔案是否到期, 發生錯誤印出訊息
			ex.printStackTrace();
			// 寫入log ????
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
				return null;
			}
			
			// 檢核record_Date + upload_No
			if (strAry[0].length() != 11) {
				System.out.println("格式不正確, 前字長度不足。"); // for test
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
				return null;
			}
			
			// 回傳  "(資料日期)|(上傳批號)|(zip檔名)"
			return record_Date + "|" + upload_No + "|" + zipFileName;
			
		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("發生錯誤"); // for test
			return null;
		}
	}
	
	// 寫入Master Log
	private static boolean hasMasterLog(Date record_date, String central_No, String upload_No) {
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.query_Master_Log(?,?,?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(record_date.getTime()));
			cstmt.setString(3, central_No);
			cstmt.setString(4, upload_No);
			cstmt.registerOutParameter(5, Types.VARCHAR);
			cstmt.registerOutParameter(6, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(6);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
//				throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            
	            // 發生錯誤  回傳true 視為已經執行過，避免不必要的寫入
				return true;
			}
			
			String hasMaster = cstmt.getString(5);
			if ("Y".equals(hasMaster)) {
				return true;
			} else {
				return false;
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
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
			return true;
		} else {
			
			System.out.println("刪除 " + remoteMasterFile + " 失敗！");
			return false;
		}
		
	}
	
	public static void main(String[] args) {
		execute();
	}

}
