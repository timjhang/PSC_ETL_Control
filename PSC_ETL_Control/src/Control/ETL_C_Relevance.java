package Control;

import java.sql.CallableStatement;
import java.sql.Connection;
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

public class ETL_C_Relevance {
	
	// ETL 關聯性檢查批次
	public static void execute() {
		
		System.out.println("####ETL_C_Relevance 關聯性檢查  Start " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
		Date record_date;
		try {
			record_date = ETL_C_Master.getBeforeRecordDate(new Date());
//			record_date = new SimpleDateFormat("yyyyMMdd").parse("20180906");
		} catch (Exception ex) {
			ex.printStackTrace();
        	System.out.println("####ETL_C_Relevance 無法取得資料日期，無法繼續進行！");
        	ETL_P_Log.write_Runtime_Log("ETL_C_Relevance", "####ETL_C_Relevance 無法取得資料日期，無法繼續進行！");
        	
        	// 寫入執行成功信件
			String mailContent = 
					"ETL_C_Relevance  關聯性檢查批次  無法取得資料日期";
			ETL_Tool_Mail.writeAML_Mail("SYSAdmin", null, null, "AML_ETL 系統通知", mailContent);
        	return;
		}
		
		// 上班日執行
		if (ETL_C_Supervision.isBusinessDay(new Date())) {
			
			// 取得每日批次編號
			String batch_No = ETL_C_Master.getETL_BatchNo();
			
			// 取得所有中心
			List<String> centralList = getRunCentrlList();
			
			// 取得可用ETL Server
			List<String[]> etlServerList = ETL_C_Master.getUsableETLServer("ETL_SERVER", "Y");
			
			// 檢核ETL Server 是否正常可連線
			// 排除連線異常，不可使用ETL Server，並給出提示訊息
			ETL_C_Master.filterETLServerOK(etlServerList);
			
			if (etlServerList.size() == 0) {
				System.out.println("#### ETL_C_Relevance 無可用ETL Server 不進行作業");
				ETL_P_Log.write_Runtime_Log("ETL_C_Relevance", "#### ETL_C_Relevance 無可用ETL Server 不進行作業");
				return;
			}
			
			// **更新 Server 狀態使用中
			try {
				ETL_C_PROCESS.update_Server_Status(etlServerList.get(0)[0], "U");
			} catch (Exception ex) {
				System.out.println("#### ETL_C_Relevance 更新Server狀態\"使用中\"失敗 " + etlServerList.get(0)[0]);
				ETL_P_Log.write_Runtime_Log("ETL_C_Relevance", "#### ETL_C_Relevance 更新Server狀態\"使用中\"失敗 " + etlServerList.get(0)[0]);
				ex.printStackTrace();
				return;
			}
			
			try {
//				ETL_C_CallWS.call_Relevance(etlServerList.get(0)[2], batch_No, "600", new SimpleDateFormat("yyyy-MM-dd").format(record_date));
				
				for (int i = 0; i < centralList.size(); i++) {
					ETL_C_CallWS.call_Relevance(etlServerList.get(0)[2], batch_No, centralList.get(i), new SimpleDateFormat("yyyy-MM-dd").format(record_date));
				}
			} catch (Exception ex) {
				ex.printStackTrace();
				System.out.println(ex.getMessage());
			} finally {
				// **更新 Server 狀態可使用
				try {
					ETL_C_PROCESS.update_Server_Status(etlServerList.get(0)[0], "Y");
				} catch (Exception ex) {
					System.out.println("#### ETL_C_Relevance 更新Server狀態\"使用中\"失敗 " + etlServerList.get(0)[0]);
					ETL_P_Log.write_Runtime_Log("ETL_C_Relevance", "#### ETL_C_Relevance 更新Server狀態\"使用中\"失敗 " + etlServerList.get(0)[0]);
					ex.printStackTrace();
					return;
				}
			}
		}
		
		System.out.println("####ETL_C_Relevance 關聯性檢查  End " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
	}
	
	// 取得所有進行中心
	private static List<String> getRunCentrlList() {
		List<String> resultList = new ArrayList<String>();
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.getRunCentral(?,?,?)}";
			
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
	            ETL_P_Log.write_Runtime_Log("getRunCentrlList", "Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            
	            return resultList;
			}
			
			java.sql.ResultSet rs = (java.sql.ResultSet) cstmt.getObject(2);
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
			ETL_P_Log.write_Runtime_Log("getRunCentrlList", ex.getMessage());
		}
		
		return resultList;
	}
	
}
