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

import DB.ConnectionHelper;
import DB.ETL_P_Log;
import FTP.ETL_SFTP;
import Profile.ETL_Profile;
import Tool.ETL_Tool_FormatCheck;

public class ETL_C_Rerun {
	
	public static void execute() {
		
		Calendar c1 = Calendar.getInstance();
    	final String strTime = String.format("%1$tH%1$tM", c1);
    	
    	//  使用編號" 3"設定檔(BatchRunTimeConfig)
    	boolean isRun = ETL_C_BatchTime.isExecute(strTime, " 3");
    	
    	if (!isRun) {
    		System.out.println("ETL_C_Rerun skip");
    		return;
    	}
		
    	System.out.println("####ETL_C_Rerun Start " + new Date());
    	
    	// 檢視是否ETL執行中, 有則返回
    	// 正常情況 Rerun ETL若執行中  是不可以註冊rerun的, 所以這一段應沒有問題
    	
    	try {
    	
	    	// 檢視是否有rerun被註冊執行, 取得中心進行Rerun
	    	String[] rerun_Central_Ary = new String[1];
	    	List<Date> rerunDateList = getRerunCentral(rerun_Central_Ary);
	    	String rerun_Central_No = rerun_Central_Ary[0];
	    	
	    	if (rerun_Central_No == null || rerunDateList == null || rerunDateList.size() == 0) {
	    		System.out.println("無需要Rerun單位，不進行作業。");
	    		ETL_P_Log.write_Runtime_Log("ETL_C_Rerun", "無需要Rerun單位，不進行作業。");
	    		return;
	    	}
	    	
	    	// 取得Rerun批次編號(序號表ETL_PARAMETER_INFO)
    		String batchNo = getRerun_BatchNo();
    		System.out.println("BatchNo = " + batchNo); // for test
    		ETL_P_Log.write_Runtime_Log("ETL_C_Rerun", "BatchNo = " + batchNo);
	    	
	    	// 對不同天進行rerun
	    	for (int date_index_i = 0; date_index_i < rerunDateList.size(); date_index_i++) {
	    		
	    		Date rerunRecordDate = rerunDateList.get(date_index_i);
	    		System.out.println("進行rerun日期：" + rerunRecordDate);
	    		ETL_P_Log.write_Runtime_Log("ETL_C_Rerun", "進行rerun日期：" + rerunRecordDate);
	    		
	    		// 確認是否有Rerun檔(function有提示訊息)
	    		if (!checkHasRerunTxt(rerun_Central_No, rerunRecordDate)) {
	    			continue;
	    		}
	    		
	    		// 取得可用ETL Server
	    		List<String[]> etlServerList = ETL_C_Master.getUsableETLServer("ETL_SERVER", "Y");
	    		
	    		// 檢核ETL Server 是否正常可連線
	    		// 排除連線異常，不可使用ETL Server，並給出提示訊息
	    		ETL_C_Master.filterETLServerOK(etlServerList);
	    		
	    		if (etlServerList.size() == 0) {
	    			System.out.println("#### 無可用ETL Server 不進行作業");
	    			ETL_P_Log.write_Runtime_Log("ETL_C_Rerun", "#### 無可用ETL Server 不進行作業");
	    			continue;
	    		}
	    		
	    		// 呼叫確認用Web Service連線
	     		System.out.println("etlServerList size = " + etlServerList.size()); // for test
	    		System.out.println("Usable ETL Server List :");
	    		for (int i = 0; i < etlServerList.size(); i++) {
	    			System.out.println("Server_No : " + etlServerList.get(i)[0] + " , Server : " + etlServerList.get(i)[1] + " , IP : " + etlServerList.get(i)[2]);
	    		}
	    		
	    		// 清除可能留下load rerun Table
	    		if (!ETL_C_New5G.clearLoadTable(rerun_Central_No, "RERUN")) {
	    			
	    			System.out.println("清除相關Rerun Table 發生錯誤!");
	    			ETL_P_Log.write_Runtime_Log("ETL_C_Rerun", "清除相關Rerun Table 發生錯誤!");
	    			continue;
	    		}
	    		
	    		Date beforeRecordDate = new Date();
	    		try {
	    			beforeRecordDate = ETL_C_Master.getBeforeRecordDate(rerunRecordDate);
	    		} catch (Exception ex) {
	    			ex.printStackTrace();
	    			ETL_P_Log.write_Runtime_Log("ETL_C_Rerun", 
	    					new SimpleDateFormat("yyyy-MM-dd").format(rerunRecordDate) + "取得前一資料日期失敗\n" + ex.getMessage());
	    			continue;
	    		}
	    		
	    		// 建立新一代load rerun Table
	    		if (!ETL_C_FIVE_G.generateNewGTable(beforeRecordDate, rerunRecordDate, rerun_Central_No, "RERUN")) {
	    			System.out.println("####ETL_C_Rerun - 建立 " + rerun_Central_No + " 新一代Rerun Table 資料日期" 
	    					+ new SimpleDateFormat("yyyy-MM-dd").format(rerunRecordDate) + "發生錯誤！");
	    		}
	    		
//	    		// 取得Rerun批次編號(序號表ETL_PARAMETER_INFO)
//	    		String batchNo = getRerun_BatchNo();
//	    		System.out.println("BatchNo = " + batchNo); // for test
//	    		ETL_P_Log.write_Runtime_Log("ETL_C_Rerun", "BatchNo = " + batchNo);
	    		
	    		// 取得 List<資料日期|上傳批號 |zip檔名>
	    		List<String> zipFiles = new ArrayList<String>();
	    		zipFiles = parseRerunTxtContent(rerun_Central_No, rerunRecordDate);
	    		String[] dataInfo = zipFiles.get(0).split("\\|");
	    		
	    		// 確認解析出來的內容是否可執行
	    		// 確認是否為正確資料日期
				if (dataInfo[0].equals(new SimpleDateFormat("yyyyMMdd").format(rerunRecordDate))) {
					System.out.println(rerun_Central_No + " Rerun 檔資料日期正確！");
					ETL_P_Log.write_Runtime_Log("checkRerunCentral", rerun_Central_No + " Rerun 檔資料日期正確！");
					
					// 確認是否為執行過資料(Master Log), 未執行過資料才列入待執行清單
					if (ETL_C_Master.hasMasterLog(rerunRecordDate, rerun_Central_No, dataInfo[1])) {
						System.out.println(rerun_Central_No + " " + dataInfo[1] + "已經執行過！"); // for test
						ETL_P_Log.write_Runtime_Log("checkReadyCentral", rerun_Central_No + " " + dataInfo[1] + "已經執行過！");
						
						// 將已經執行過的Rerun txt刪除
						removeRerunTxt(rerun_Central_No, rerunRecordDate);
						continue;
					}
					
				} else {
					System.out.println(rerun_Central_No + " Rerun 檔資料日期非預期:" + dataInfo[0]); // for test
					ETL_P_Log.write_Runtime_Log("checkReadyCentral", rerun_Central_No + " Rerun 檔資料日期非預期:" + dataInfo[0]);
					
					// 將錯誤的Rerun txt刪除
					removeRerunTxt(rerun_Central_No, rerunRecordDate);
					continue;
				}
				
				String[] ptr_upload_no = new String[1];
				
	    		// 指定ETL任務
	    		ETL_C_PROCESS.executeRerun(etlServerList.get(0), batchNo, rerun_Central_No, ptr_upload_no, rerunRecordDate, beforeRecordDate);
	    		
	    		boolean add5GSuccess = false;
	    		
	    		// 執行暫存Table(load_rerun)併入五代
	    		add5GSuccess = ETL_C_Master.addNew5G(batchNo, rerunRecordDate, rerun_Central_No, ptr_upload_no[0], "RERUN");
	    		
	    		// 執行runstate程式
	    		ETL_C_Master.runStateSRC(rerun_Central_No);
	    		
	    		// 執行正常完整, 則寫入
	    		if (add5GSuccess) {
	    			
	    			// 寫入ETL完成紀錄ETL_LOAD_GAML
	    			boolean isSuccess;
	    			isSuccess = write_rerun_ETL_LOAD_GAML(batchNo, rerunRecordDate, rerun_Central_No, ptr_upload_no[0]);
	    			
	    			System.out.println("batchNo = " + batchNo + " , record_date = " + rerunRecordDate 
	    					+ " , central_no = " + rerun_Central_No + " , upload_no = " + ptr_upload_no[0]);
	    			if (isSuccess) {
	    				System.out.println("寫入ETL_LOAD_GAML成功!");
	    				ETL_P_Log.write_Runtime_Log("ETL_C_Master", "寫入ETL_LOAD_GAML成功!");
	    			} else {
	    				System.out.println("寫入ETL_LOAD_GAML失敗!");
	    				ETL_P_Log.write_Runtime_Log("ETL_C_Master", "寫入ETL_LOAD_GAML失敗!");
	    			}
	    			
	    		}
	    		
	    		// 跑完的時候關閉對應的Flag
	    		updateRerunStatus(rerun_Central_No, rerunRecordDate, "");
	    		
	    	}
    	
    	} catch (Exception ex) {
    		ex.printStackTrace();
    		ETL_P_Log.write_Runtime_Log("ETL_C_Rerun", ex.getMessage());
    	}
    	
    	System.out.println("####ETL_C_Rerun End " + new Date());
	}
	
	// 取得rerun中心, 與對應資料日期
	private static List<Date> getRerunCentral(String[] rerunCentral) {
		
		List<Date> resultList = new ArrayList<Date>();
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.getRerunCentral(?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.registerOutParameter(2, Types.VARCHAR);
			cstmt.registerOutParameter(3, DB2Types.CURSOR);
			cstmt.registerOutParameter(4, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(4);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
//	            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            ETL_P_Log.write_Runtime_Log("getRerunCentral", "Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            
	            return resultList;
			}
			
			if (cstmt.getString(2) != null) {
				rerunCentral[0] = cstmt.getString(2).trim();
			} else {
				// 若無執行單位, 直接返回
				return resultList;
			}
			
			java.sql.ResultSet rs = (java.sql.ResultSet) cstmt.getObject(3);
			while (rs.next()) {
				if (rs.getDate(1) != null) {
					Date recordDate = new java.util.Date(rs.getDate(1).getTime());
	        		resultList.add(recordDate);
	        	}
	        }
	        
//	        System.out.println("List Size = " + resultList.size()); // for test
		
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("getRerunCentral", ex.getMessage());
		}
		
		return resultList;
	}
	
	// 取得Rerun批次編號(序號表ETL_PARAMETER_INFO)
	private static String getRerun_BatchNo() {
		
		String result = "";
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.getRerun_BatchNo(?,?,?)}";
			
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
//		            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            ETL_P_Log.write_Runtime_Log("getRerun_BatchNo", "Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            
	            return result;
			}
			
			result = cstmt.getString(2);
	        
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("getETL_BatchNo", ex.getMessage());
		}
		
		return result;
	}
	
	// 確認是否有對應Rerun檔
	private static boolean checkHasRerunTxt(String central_No, Date record_Date) throws Exception {
		
		// 組成目標Master檔名
		String masterFileName = central_No + "_RERUN_" + new SimpleDateFormat("yyyyMMdd").format(record_Date) + ".txt";
		String remoteFilePath = "/" + central_No + "/UPLOAD/";
		String remoteMasterFile = remoteFilePath + masterFileName;
		boolean hasMaster = ETL_SFTP.exist(ETL_C_Profile.sftp_hostName, ETL_C_Profile.sftp_port, 
				ETL_C_Profile.sftp_username, ETL_C_Profile.sftp_password, remoteMasterFile);
		
		if (!hasMaster) {
			System.out.println("找不到" + remoteMasterFile + "檔案!");
			ETL_P_Log.write_Runtime_Log("checkHasRerunTxt", "找不到" + remoteMasterFile + "檔案!"); // for test
			return false;
		}
		
		return true;
	}
	
	// 解析Rerun檔內容
	private static List<String> parseRerunTxtContent(String central_No, Date record_Date) throws Exception {
		
		// 結果字串
		List<String> resultList = new ArrayList<String>();
		
		String masterFileName = central_No + "_RERUN_" + new SimpleDateFormat("yyyyMMdd").format(record_Date) + ".txt";
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
				+ "/" + central_No + "_RERUN_" + new SimpleDateFormat("yyyyMMdd").format(record_Date) + "_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".txt";
		
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
		
		String rerunLineStr = "";
		String resultStr = "";
		while (br.ready()) {
			rerunLineStr = br.readLine();
			System.out.println(rerunLineStr); // for test
			
			resultStr = checkRerunLineString(central_No, rerunLineStr);
			if (resultStr == null) {
				throw new Exception("解析" + parseFile.getName() + "解析出現問題!");
			}
			
			resultList.add(resultStr);
		}
		
		br.close();
		fis.close();
		
		return resultList;
	}
	
	// 解析檢核Rerun File當中String
	private static String checkRerunLineString(String central_No, String input) {
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
	
	// 刪除SFTP上XXX_RERUN_yyyyMMdd.txt檔
	private static boolean removeRerunTxt(String central_no, Date record_date) {
		
		String rerunFileName = central_no + "_RERUN_" + new SimpleDateFormat("yyyyMMdd").format(record_date) + ".txt";
		String remoteFilePath = "/" + central_no + "/UPLOAD/";
		String remoteMasterFile = remoteFilePath + rerunFileName;
		
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
	
	// 更新Rerun Flag
	private static boolean updateRerunStatus(String central_no, Date record_date, String status) {
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.updateRerunStatus(?,?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setString(2, central_no);
			cstmt.setDate(3, new java.sql.Date(record_date.getTime()));
			cstmt.setString(4, status);
			cstmt.registerOutParameter(5, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(5);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
//		            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            ETL_P_Log.write_Runtime_Log("updateRerunStatus", "Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            
	            return false;
			}
			
			return true;
	        
		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("updateRerunStatus", ex.getMessage());
			return false;
		}
	}
	
	// 寫入ETL完成紀錄Rerun
	private static boolean write_rerun_ETL_LOAD_GAML(String batch_no, Date record_date, String central_no, String upload_no) {
		
		try {
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.write_rerun_ETL_LOAD_GAML(?,?,?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setString(2, central_no);
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
	
	public static void main(String[] args) {
//		String[] central_no = new String[1];
//		List<Date> list;
//		
//		list = getRerunCentral(central_no);
//		
//		System.out.println("central_no = " + central_no[0]);
//		for (int i = 0; i < list.size(); i++) {
//			System.out.println(list.get(i));
//		}
		
//		System.out.println(getRerun_BatchNo());
		try {
			Date date = new SimpleDateFormat("yyyyMMdd").parse("20180502");
			Date date2 = new SimpleDateFormat("yyyyMMdd").parse("20180503");
		
			System.out.println(date);
			System.out.println(date2);
			
			System.out.println(date.before(date2));
			System.out.println(date2.before(date));
			System.out.println(date.before(date));
		
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
	}

}
