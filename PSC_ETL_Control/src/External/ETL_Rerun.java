package External;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.ibm.db2.jcc.DB2Types;

import DB.ConnectionHelper;
import Profile.ETL_Profile;

public class ETL_Rerun {
	
	// 確認是否可執行rerun
	public static boolean checkRerunOK(String[] errorMsg) {
		
		// 取得可用ETL Server
		List<String[]> etlServerList = getUsableETLServer("ETL_SERVER", "Y");
//		System.out.println("可用ETL Server " + etlServerList.size()); // for test
		
		// 是否有可用ETL Server檢查
		if (etlServerList == null || etlServerList.size() == 0) {
			errorMsg[0] = "E1";
			errorMsg[1] = "無可使用ETL Server, 無法執行。";
			errorMsg[2] = "";
			
			return false;
		}
		
		// 若rerun進行中, 不可執行rerun
		if (isRerunExecute()) {
			errorMsg[0] = "E2";
			errorMsg[1] = "正在進行rerun作業, 執行失敗。";
			errorMsg[2] = "";
			return false;
		}
		
		String[] status = new String[1];
		String[] falseMsg = new String[1];
		// 若ETL進行中, 不可進行rerun
		if (isETLplanExecute(status, falseMsg)) {
			if (!"EE".equals(falseMsg)) {
				errorMsg[0] = status[0];
				errorMsg[1] = falseMsg[0];
				errorMsg[2] = "";
			} else {
				errorMsg[0] = "EE";
				errorMsg[1] = "執行發生系統錯誤, 執行失敗。";
				errorMsg[2] = falseMsg[0];
			}
			
			return false;
		}
		
		errorMsg[0] = "";
		errorMsg[1] = "";
		errorMsg[2] = "";

		return true;
	}
	
	// 執行rerun作業
	public static boolean executeRerun(String central_No, String[] rerunRecordDates, String personID, String[] errorMsg) {
		
		String[] partition_Info = new String[3];
		if (!guery_GAML_Partition_Info(central_No, partition_Info)) {
			errorMsg[0] = "E6";
			errorMsg[1] = "取得" + central_No + "五代分割訊息出現錯誤，返回不繼續執行！";
			errorMsg[2] = "";
			return false;
		}
		
		Date oldestRecordDate;
		Date newRecordDate;
		
		try {
			// 可rerun最早的上班日
			oldestRecordDate = new SimpleDateFormat("yyyyMMdd").parse(partition_Info[1]);
			// 可rerun最晚的上班日
			newRecordDate = getNextRecordDate(new SimpleDateFormat("yyyyMMdd").parse(partition_Info[2]));
		} catch (Exception ex) {
			errorMsg[0] = "E7";
			errorMsg[1] = "取得" + central_No + "五代分割日期出現錯誤，返回不繼續執行！";
			errorMsg[2] = ex.getMessage();
			return false;
		}
		
		// 確認是否有rerun日期
		if (rerunRecordDates == null || rerunRecordDates.length == 0) {
			errorMsg[0] = "E8";
			errorMsg[1] = "無rerun日期，返回不繼續執行。";
			errorMsg[2] = "";
			return false;
		}
		
		Date[] recordDates = new Date[rerunRecordDates.length];
		int count = 0;
		for (int i = 0; i < rerunRecordDates.length; i++) {
			
			try {
				Date recordDate = new SimpleDateFormat("yyyyMMdd").parse(rerunRecordDates[i]);
				
				if (!oldestRecordDate.before(recordDate) || !newRecordDate.after(recordDate)) {
					errorMsg[0] = "E10";
					errorMsg[1] = rerunRecordDates[i] + "超出可rerun日期範圍，無法進行rerun。";
					errorMsg[2] = "";
					return false;
				}
				
				if (!isBusinessDay(recordDate)) {
					errorMsg[0] = "E11";
					errorMsg[1] = rerunRecordDates[i] + "為非營業日，無法進行rerun。";
					errorMsg[2] = "";
					return false;
				}
				
				recordDates[count] = recordDate;
				count++;
			} catch (Exception ex) {
				errorMsg[0] = "E9";
				errorMsg[1] = rerunRecordDates[i] + "非日期格式，無法進行rerun。";
				errorMsg[2] = "";
				return false;
			}
		}
		
		// 取得排序過Date Array
		Date[] recordDateArray = getSortDateArray(recordDates);
		
		// 逐項註冊中心資料日期rerun
		for (int i = 0; i < recordDateArray.length; i++) {
			try {
				updateRerunFlag(central_No, recordDateArray[i], personID);
			} catch (Exception ex) {
				errorMsg[0] = "E12";
				errorMsg[1] = recordDateArray[i] + "更新Rerun Flag失敗，後續營業日未進行Rerun。";
				errorMsg[2] = "";
				return false;
			}
		}
		
		errorMsg[0] = "";
		errorMsg[1] = "";
		errorMsg[2] = "";

		return true;
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
	        
//		        System.out.println("List Size = " + resultList.size()); // for test
		
		} catch (Exception ex) {
			ex.printStackTrace();
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
		        System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
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
			return false;
		}
	}
	
	// 判斷是否ETL作業進行中
	private static boolean isETLplanExecute(String[] status, String[] falseMsg) {
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.isETLplanExecute(?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.registerOutParameter(2, Types.VARCHAR);
			cstmt.registerOutParameter(3, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			String statusCode = cstmt.getString(2);
			String errorMessage = cstmt.getString(3);
			
			// 回傳999表ETL Daily Run or 備檔  or Migration執行中
			if (returnCode == 999) {
				status[0] = statusCode;
				falseMsg[0] = errorMessage;
				return true;
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			} else if (returnCode != 0) {
		        System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
		        status[0] = "EE";
		        falseMsg[0] = "Error Code = " + returnCode + ", Error Message : " + errorMessage;
		        return true;
			}
			
			return false;
		} catch (Exception ex) {
			ex.printStackTrace();
			status[0] = "EE";
	        falseMsg[0] = ex.getMessage();
			return true;
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
	            return false;
			}
			
			int partiCount = cstmt.getInt(3);
			String partiFirstName = cstmt.getString(4);
			String partiLastName = cstmt.getString(5);
			
//			System.out.println("partiCount = " + partiCount); // for test
//			System.out.println("partiFirstName = " + partiFirstName); // for test
//			System.out.println("partiLastName = " + partiLastName); // for test
			
			partition_Info[0] = String.valueOf(partiCount);
			partition_Info[1] = partiFirstName;
			partition_Info[2] = partiLastName;
			
			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			return false;
		}
	}
	
	// 確認是否為營業日
	private static boolean isBusinessDay(Date recordDate) {
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.isBusinessDay(?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
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
		}
	}
	
	// 查詢下一個資料日期
	private static Date getNextRecordDate(Date recordDate) throws Exception {
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.getNext_RecordDate(?,?,?,?)}";
			
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
//		        System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}
			
			return new java.util.Date(cstmt.getDate(3).getTime());
			
//		    System.out.println("List Size = " + resultList.size()); // for test
		
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new Exception("無法取得下一代資料日期!");
		}
	}
	
	// 取得排序後資料日期Array
	private static Date[] getSortDateArray(Date[] ary) {
		
		int count = 0;
		for (int i = 0; i < ary.length; i++) {
			if (ary[i] != null) {
				count++;
			}
		}
		
		Date[] dateAry = new Date[count];
		
		int index = 0;
		for (int i = 0; i < ary.length; i++) {
			if (ary[i] != null) {
				dateAry[index] = ary[i];
				index++;
			}
		}
		
		Date tempDate;
		int minIndex = 0;
		
		for (int i = 0; i < dateAry.length; i++) {
			minIndex = i;
			for (int j = i + 1; j < dateAry.length;j++) {
				if (dateAry[j].before(dateAry[minIndex])) {
					minIndex = j;
				}
			}
			
			if (minIndex != i) {
				tempDate = dateAry[minIndex];
				dateAry[minIndex] = dateAry[i];
				dateAry[i] = tempDate;
			}
		}
		
		return dateAry;
	}
	
	// 註冊進行Rerun
	private static void updateRerunFlag(String centralNo, Date recordDate, String personID) throws Exception {
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.updateRerunFlag(?,?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setString(2, centralNo);
			cstmt.setDate(3, new java.sql.Date(recordDate.getTime()));
			cstmt.setString(4, personID);
			cstmt.registerOutParameter(5, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(5);
//		        System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
			throw new Exception(ex.getMessage());
		}
	}
	
	public static void main(String[] argv) throws Exception {
		
		/** 測試  是否可進行ETL Rerun **/
		String[] errorMsg = new String[3]; // 必須
		
//		errorMsg[0] = ""; // 測試用(正式使用不須)
		
		if (!ETL_Rerun.checkRerunOK(errorMsg)) {
			// 無法正常執行, 印出錯誤訊息
			System.out.println(errorMsg[0]); // 錯誤代碼
			System.out.println(errorMsg[1]); // 錯誤中文說明(for IT)
			System.out.println(errorMsg[2]); // Exception Content
			System.out.println("確認失敗");
		} else {
			// 確認ok可以正常執行
			System.out.println("確認OK!!");
		}
		
//		String[] partiton_info = new String[3]; // 必須
//		guery_GAML_Partition_Info("018", partiton_info);
//		System.out.println(partiton_info[0]);
//		System.out.println(partiton_info[1]);
//		System.out.println(partiton_info[2]);
		
//		System.out.println(isBusinessDay(new SimpleDateFormat("yyyyMMdd").parse("20180813")));
		
//		System.out.println(getNextRecordDate(new SimpleDateFormat("yyyyMMdd").parse("20180809")));
		
//		Date[] dateArray = new Date[6];
//		dateArray[0] = new SimpleDateFormat("yyyyMMdd").parse("20180214");
//		dateArray[1] = new SimpleDateFormat("yyyyMMdd").parse("20180101");
//		dateArray[2] = new SimpleDateFormat("yyyyMMdd").parse("20180228");
//		dateArray[3] = new SimpleDateFormat("yyyyMMdd").parse("20181010");
//		dateArray[4] = new SimpleDateFormat("yyyyMMdd").parse("20180808");
//		dateArray[5] = null;
//		Date[] ary = getSortDateArray(dateArray);
//		System.out.println(ary.length);
//		for (int i = 0; i < ary.length; i++) {
//			System.out.println(ary[i]);
//		}
		
		String[] dateArray = new String[3];
		dateArray[0] = "20180628";
		dateArray[1] = "20180625";
		dateArray[2] = "20180626";
//		executeRerun("018", dateArray, "Tim", errorMsg);
		
		if (!executeRerun("018", dateArray, "Tim", errorMsg)) {
			// 無法正常執行, 印出錯誤訊息
			System.out.println(errorMsg[0]); // 錯誤代碼
			System.out.println(errorMsg[1]); // 錯誤中文說明(for IT)
			System.out.println(errorMsg[2]); // Exception Content
			System.out.println("執行失敗");
		} else {
			// 確認ok可以正常執行
			System.out.println("執行OK!!");
		}
		
		/** 測試  進行ETL Rerun **/
//		String central_No = "018";
//		String[] rerunRecordDates = {"20180221", "20180222", "20180223"};
//		String userID = "10508001";
//		String[] errorMsg2 = new String[3]; // 必須
//		errorMsg2[0] = "EE"; // 測試用(正式使用不須)
//		
//		if (!ETL_Rerun.executeRerun(central_No, rerunRecordDates, userID, errorMsg2)) {
//			// 無法正常執行, 印出錯誤訊息
//			System.out.println(errorMsg[0]); // 錯誤代碼
//			System.out.println(errorMsg[1]); // 錯誤中文說明(for IT)
//			System.out.println(errorMsg[2]); // Exception Content
//			System.out.println("執行失敗");
//		} else {
//			// 確認ok可以正常執行
//			System.out.println("執行OK!!");
//		}
	}

}
