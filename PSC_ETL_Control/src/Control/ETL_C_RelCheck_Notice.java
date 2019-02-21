package Control;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.ibm.db2.jcc.DB2Types;

import Bean.ETL_Bean_LogData;
import Bean.ETL_Bean_MailReceiveUnit;
import DB.ConnectionHelper;
import DB.ETL_P_Log;
import Profile.ETL_Profile;
import Tool.ETL_Tool_Mail;

public class ETL_C_RelCheck_Notice {
	
	public static void execute() {
		
		System.out.println("#### ETL_C_RelCheck_Notice Start  " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
		// 前一個工作日
		Date record_date;
		try {
			record_date = ETL_C_Master.getBeforeRecordDate(new Date());
		} catch (Exception ex) {
			ex.printStackTrace();
			StringWriter sw = new StringWriter(); 
			PrintWriter pw = new PrintWriter(sw); 
			ex.printStackTrace(pw); 
			System.out.println("ExceptionMassage:" + sw.toString());
			
        	System.out.println("####ETL_C_RelCheck_Notice 無法取得資料日期，無法繼續進行！ " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
        	ETL_P_Log.write_Runtime_Log("ETL_C_RelCheck_Notice", "####ETL_C_RelCheck_Notice 無法取得資料日期，無法繼續進行！");
        	
        	// 寫入執行成功信件
			String mailContent = 
					"關聯性驗證提醒批次, 無法取得資料日期, 無法繼續作業。";
			ETL_Tool_Mail.writeAML_Mail("SYSAdmin", null, null, "AML_ETL 系統通知", mailContent);
        	return;
		}
		
		// 取得運行中心  及  最後完成ETL資料日期
		List<ETL_Bean_LogData> list = getRelCheckOver_CentralList(record_date);
		
		// *信件內容用資料
		String uploadJar_version = ETL_C_Comm.getGAMLDB_Profile(1);
		
		for (int i = 0; i < list.size(); i++) {
			ETL_Bean_LogData aData = list.get(i);
			
			String subject = "【AML系統通知-" + aData.getCENTRAL_NO() + "】已產出"
					+ new SimpleDateFormat("yyyy/MM/dd").format(aData.getRECORD_DATE()) + "資料關聯檢核報表";
			
			String content = 
				"\t您好\n" 
				+ "\t已產出  貴單位報送AML交換檔的" + new SimpleDateFormat("yyyy/MM/dd").format(aData.getRECORD_DATE()) + "資料關聯檢核報表，\n"
				+ "\t請使用上傳檔案程式" + uploadJar_version + "版的 “執行下載資料關聯檢核檔程式.bat”，\n"
				+ "\t取回資料關聯檢核報表。";
			
			if ("018".equals(aData.getCENTRAL_NO())) {
				ETL_Bean_MailReceiveUnit receiveUnit = new ETL_Bean_MailReceiveUnit("RelCheckReceive", "018");
				ETL_Bean_MailReceiveUnit ccUnit = new ETL_Bean_MailReceiveUnit("RelCheckCC", "018");
				ETL_Tool_Mail.writeAML_Mail(receiveUnit, ccUnit, null, subject, content);
			} else if ("600".equals(aData.getCENTRAL_NO())) {
				ETL_Bean_MailReceiveUnit receiveUnit = new ETL_Bean_MailReceiveUnit("RelCheckReceive", "600");
				ETL_Bean_MailReceiveUnit ccUnit = new ETL_Bean_MailReceiveUnit("RelCheckCC", "600");
				ETL_Tool_Mail.writeAML_Mail(receiveUnit, ccUnit, null, subject, content);
			} else {
				ETL_Bean_MailReceiveUnit receiveUnit = new ETL_Bean_MailReceiveUnit("RelCheckReceive", aData.getCENTRAL_NO());
				ETL_Bean_MailReceiveUnit ccUnit = new ETL_Bean_MailReceiveUnit("RelCheckCC", aData.getCENTRAL_NO());
				ETL_Tool_Mail.writeAML_Mail(receiveUnit, ccUnit, null, subject, content);
			}
			
		}
		
		System.out.println("#### ETL_C_RelCheck_Notice End  " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
	}
	
	// 取得運行中心  及  最後完成ETL資料日期
	private static List<ETL_Bean_LogData> getRelCheckOver_CentralList(Date record_date) {
		CallableStatement cstmt = null;
		java.sql.ResultSet rs = null;
		Connection con = null;
		
		List<ETL_Bean_LogData> resultList = new ArrayList<ETL_Bean_LogData>();
		
		try {

			String sql = "{call " + ETL_Profile.db2TableSchema + ".AML_EMAIL.getRelCheck_NoticeCentrals(?,?,?,?)}";

			con = ConnectionHelper.getDB2Connection();
			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(record_date.getTime()));
			cstmt.registerOutParameter(3, DB2Types.CURSOR);
			cstmt.registerOutParameter(4, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			// 有錯誤釋出錯誤訊息 不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(4);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
				return resultList;
			}

			// (java.sql.ResultSet)
			rs = (java.sql.ResultSet) cstmt.getObject(3);

			if (rs == null) {
				return resultList;
			}

			while (rs.next()) {
				String central_no = rs.getString(1);
				Date central_record_date = rs.getDate(2);
				ETL_Bean_LogData aData = new ETL_Bean_LogData();
				if (central_no != null) {
					aData.setCENTRAL_NO(central_no.trim());
					aData.setRECORD_DATE(central_record_date);
					resultList.add(aData);
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace();
			StringWriter sw = new StringWriter(); 
			PrintWriter pw = new PrintWriter(sw); 
			ex.printStackTrace(pw); 
			System.out.println("ExceptionMassage:" + sw.toString());
		} finally {
			try {
				// 資源後開,先關
				if (cstmt != null) {
					cstmt.close();
				}
				if (con != null) {
					con.close();
				}

				if (rs != null) {
					rs.close();
				}

			} catch (SQLException ex) {
				ex.printStackTrace();
				StringWriter sw = new StringWriter(); 
				PrintWriter pw = new PrintWriter(sw); 
				ex.printStackTrace(pw); 
				System.out.println("ExceptionMassage:" + sw.toString());
			}
		}
		
		return resultList;
	}

	public static void main(String[] args) {
		System.out.println("測試開始  " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		execute();
		System.out.println("測試結束  " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
	}

}
