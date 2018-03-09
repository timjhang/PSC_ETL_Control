package Control;

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
import FTP.ETL_SFTP;
import Profile.ETL_Profile;

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
		if (etlServerList.size() == 0) {
			System.out.println("#### 無可用ETL Server 不進行作業");
			return;
		}
		
		// 檢核ETL Server 是否正常可連線  ????
		
		
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
		try {
			record_date = new SimpleDateFormat("yyyyMMdd").parse("20171206");
		} catch (Exception ex) {
			ex.printStackTrace();
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
		List<String> readyCentralList = checkReadyCentral(noProcessCentralList);
		System.out.println("readyCentralList size = " + readyCentralList.size()); // for test
		System.out.println("ReadyCentralList :");
		for (int i = 0; i < readyCentralList.size(); i++) {
			System.out.println(readyCentralList.get(i));
		}
		
		// 指定ETL任務
		ETL_C_PROCESS.executeETL(etlServerList.get(0), batchNo, readyCentralList.get(0), record_date);
		
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
	private static List<String> checkReadyCentral(List<String> centralList) {
		List<String> resultLust = new ArrayList<String>();
		
		try {
			// 若檔案存在, 加入list表示就緒中心
			for (int i = 0; i < centralList.size(); i++) {
				if (checkHasMaster(centralList.get(i))) {
					resultLust.add(centralList.get(i));
				}
			}
		} catch (Exception ex) {
			// 查詢檔案是否到期, 發生錯誤印出訊息
			ex.printStackTrace();
			// 寫入log ????
		}
			
		return resultLust;
	}
	
	// 確認是否有對應Master檔
	private static boolean checkHasMaster(String central_No) throws Exception {
		
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
	
	public static void main(String[] args) {
		execute();
	}

}
