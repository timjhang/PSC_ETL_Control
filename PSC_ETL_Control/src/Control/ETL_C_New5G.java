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
import DB.ETL_P_Log;
import Profile.ETL_Profile;

public class ETL_C_New5G {

	public static void execute() {
		
		Calendar c1 = Calendar.getInstance();
    	final String strTime = String.format("%1$tH%1$tM", c1);
    	
    	//  使用編號" 2"設定檔(BatchRunTimeConfig)
    	boolean isRun = ETL_C_BatchTime.isExecute(strTime, " 2");
    	if (!isRun) {
    		System.out.println("ETL_C_New5G skip");
    		return;
    	}
    	
    	// 若Rerun執行中則, 則ETL正常執行等待
    	if (ETL_C_Master.isRerunExecuting()) {
    		System.out.println("Rerun 作業進行中, 不進行ETL作業。");
    		ETL_P_Log.write_Runtime_Log("ETL_C_Master", "Rerun 作業進行中, 不進行ETL作業。");
    		return;
    	}
    	
    	System.out.println("####ETL_C_New5G Start " + new Date());
    	
    	// 產生資料日期(昨天)
//		Calendar cal = Calendar.getInstance(); // 今天時間
//        cal.add(Calendar.DATE, -1); // 昨天時間
//        Date record_date = cal.getTime();
        
    	
    	
    	// for test
    	Date record_date = new Date();
		try {
			record_date = new SimpleDateFormat("yyyyMMdd").parse(ETL_Profile.Before_Record_Date_Str);
		} catch (Exception ex) {
			ex.printStackTrace();
			record_date = new Date();
		}
    	
    	// 巡視所有單位資料庫, 是否有需建立新一代Table 資料庫
		List<String> central_list;
		Date[] nextRecordDateAry = new Date[2];
		central_list = getNeedNewG_LoadTemp_Central(record_date, nextRecordDateAry);
		Date beforeRecordDate = nextRecordDateAry[0]; // 包含今天最晚的資料日期
		Date nextRecordDate = nextRecordDateAry[1]; // 今天之後最早的資料日期
		
		if (beforeRecordDate == null || nextRecordDate == null) {
			
			System.out.println("beforeRecordDate = " + beforeRecordDate + " , nextRecordDate = " + nextRecordDate);
			System.out.println("日曆檔無法提供正確資料!  不繼續進行");
			return;
		}
		
		if (central_list == null || central_list.size() == 0) {
			System.out.println("####ETL_C_New5G End count 0");
			return;
		} else {
			System.out.println("####ETL_C_New5G central_no list:");
			for (int i = 0; i < central_list.size(); i++) {
				System.out.println(central_list.get(0));
			}
		}
    	
		// 清除可能留下load temp Table
		if (!clearLoadTable(central_list.get(0), "TEMP")) {
			
			System.out.println("清除相關Load Table 發生錯誤!");
			return;
		}
		
		// 建立新一代load temp Table
		if (!ETL_C_FIVE_G.generateNewGTable(beforeRecordDate, nextRecordDate, central_list.get(0), "TEMP")) {
			System.out.println("####ETL_C_New5G - 建立" + central_list.get(0) + "新一代Table 資料日期" 
					+ new SimpleDateFormat("yyyy-MM-dd").format(nextRecordDate) + "發生錯誤！");
		}
    	
    	System.out.println("####ETL_C_New5G End " + new Date());
	}
	
	// 取得須建立load_temp table 中心
	private static List<String> getNeedNewG_LoadTemp_Central(Date record_date, Date[] nextRecordDate) {
		List<String> resultList = new ArrayList<String>();
		
		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Control.need_new_G_load_table_central(?,?,?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection();
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(record_date.getTime()));
			cstmt.registerOutParameter(3, Types.DATE);
			cstmt.registerOutParameter(4, Types.DATE);
			cstmt.registerOutParameter(5, DB2Types.CURSOR);
			cstmt.registerOutParameter(6, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(6);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
//	            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            return resultList;
			}
			
			if (cstmt.getDate(3) != null && cstmt.getDate(4) != null) {
				nextRecordDate[0] = new java.util.Date(cstmt.getDate(3).getTime());
				nextRecordDate[1] = new java.util.Date(cstmt.getDate(4).getTime());
			} else {
				nextRecordDate[0] = null;
			}
			
			java.sql.ResultSet rs = (java.sql.ResultSet) cstmt.getObject(5);
			while (rs.next()) {
	        	String central_no = rs.getString(1);
	        	if (central_no != null) {
	        		central_no = central_no.trim();
	        	}
	        	resultList.add(central_no);
	        }
		
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		return resultList;
	}
	
	// 應除Load Table
	public static boolean clearLoadTable(String central_no, String tableType) {

		try {
			
			String sql = "{call " + ETL_Profile.db2TableSchema + ".new5G_Service.clear_Load_Table(?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection(central_no);
			CallableStatement cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setString(2, tableType);
			cstmt.registerOutParameter(3, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			// 有錯誤釋出錯誤訊息   不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(3);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
//	            throw new Exception("Error Code = " + returnCode + ", Error Message : " + errorMessage);
	            return false;
			}
			
			System.out.println("刪除" + central_no + " load_temp 成功!!");
			return true;
			
		} catch (Exception ex) {
			ex.printStackTrace();
			return false;
		}
	}
	
	
	public static void main(String[] argvs) {
		
		try {
			
//			Date[] dAry = new Date[2];
//			
//			List<String> list = getNeedNewG_LoadTemp_Central(new SimpleDateFormat("yyyyMMdd").parse("20180411"), dAry);
//			
//			System.out.println(dAry[0]);
//			System.out.println(dAry[1]);
//			System.out.println(list.size());
//			
//			if (list != null && list.size() > 0) {
//				for (int i = 0; i < list.size(); i++) {
//					System.out.println(list.get(i));
//				}
//			}
			
//			clearLoadTable("018", "TEMP");
			
			///////
			
//			System.out.println("測試開始");
//			
//			// 建立新一代load temp Table
//			if (!ETL_C_FIVE_G.generateNewGTable(new SimpleDateFormat("yyyyMMdd").parse(ETL_Profile.Before_Record_Date_Str), 
//					new SimpleDateFormat("yyyyMMdd").parse(ETL_Profile.Record_Date_Str), "018", "TEMP")) {
//				
//				System.out.println("Error");
//			} else {
//				System.out.println("done");
//			}
//			
//			System.out.println("測試結束");
			
		} catch (Exception ex) {
			ex.getStackTrace();
		}
		
	}
	
}
