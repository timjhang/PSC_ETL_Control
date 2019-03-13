package Control;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import Bean.ETL_Bean_LogData;
import DB.ConnectionHelper;
import Profile.ETL_Profile;

public class ETL_C_DelOldTransaction {

	// 刪除中心舊交易性資料
	public static void execute() {
		
		System.out.println("####ETL_C_DelOldTransaction  刪除中心舊交易性資料  Start " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
		// 執行參數設定"Y"才放行
		String execute_Flag = ETL_C_Comm.getGAMLDB_Profile(2);
		if (!"Y".equals(execute_Flag)) {
			System.out.println("####ETL_C_DelOldTransaction  設定參數為\"" + execute_Flag + "\"不執行！");
			return;
		}
		
		// 當天為上班日則不執行
		boolean isBusinessDay = ETL_C_Supervision.isBusinessDay(new Date());
		if (isBusinessDay) {
			System.out.println("####ETL_C_DelOldTransaction  今日為上班日不執行！");
			return;
		}
		
		// 取得最近資料日期已處理完中心
		Date record_date;
		try {
			record_date = ETL_C_Master.getBeforeRecordDate(new Date());
		} catch (Exception ex) {
			ex.printStackTrace();
			StringWriter sw = new StringWriter(); 
			PrintWriter pw = new PrintWriter(sw); 
			ex.printStackTrace(pw); 
			System.out.println("ExceptionMassage:" + sw.toString());
			System.out.println("####ETL_C_DelOldTransaction  無法取得最近資料日期，不繼續執行！");
			return;
		}
		List<ETL_Bean_LogData> list = ETL_C_RelCheck_Notice.getRelCheckOver_CentralList(new Date());
		for (int i = 0; i < list.size(); i++) {
			ETL_Bean_LogData aData = list.get(i);
			// 若最近的資料日期未執行完畢, 則將該單位移除執行名單
			if (record_date.compareTo(aData.getRECORD_DATE()) != 0) {
				list.remove(i);
				i--;
			}
		}
		if (list.size() == 0) {
			System.out.println("####ETL_C_DelOldTransaction  無可執行刪除中心  不執行!");
			return;
		}
		
		// 600在遇到Data Migration時, 不進行批次
		if (!isExeDelOldTransaction(record_date)) {
			System.out.println("####ETL_C_DelOldTransaction  執行Data Migration  不執行!");
			return;
		}
		
		// 執行維護(中心單線排序)
		for (int i = 0; i < list.size(); i++) {
			exeDelOldTransaction(list.get(i).getCENTRAL_NO());
		}
		
		System.out.println("####ETL_C_DelOldTransaction  刪除中心舊交易性資料  End " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
	}
	
	// 600在遇到Data Migration時, 不進行批次
	private static boolean isExeDelOldTransaction(Date record_date) {
		CallableStatement cstmt = null;
		Connection con = null;
		
		try {
			String sql = "{call " + ETL_Profile.db2TableSchema + ".TRANSACTION_CONTROL_SERVICE.isExeDelOldTransaction(?,?,?,?)}";

			con = ConnectionHelper.getDB2Connection();
			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setDate(2, new java.sql.Date(record_date.getTime()));
			cstmt.registerOutParameter(3, Types.VARCHAR);
			cstmt.registerOutParameter(4, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			// 有錯誤釋出錯誤訊息 不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(4);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}

			String result = cstmt.getString(3);
			if ("Y".equals(result)) {
				return true;
			} else {
				return false;
			}

		} catch (Exception ex) {
			ex.printStackTrace();
			StringWriter sw = new StringWriter(); 
			PrintWriter pw = new PrintWriter(sw); 
			ex.printStackTrace(pw); 
			System.out.println("ExceptionMassage:" + sw.toString());
			return false;
		} finally {
			try {
				// 資源後開,先關
				if (cstmt != null) {
					cstmt.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (SQLException ex) {
				ex.printStackTrace();
				StringWriter sw = new StringWriter(); 
				PrintWriter pw = new PrintWriter(sw); 
				ex.printStackTrace(pw); 
				System.out.println("ExceptionMassage:" + sw.toString());
			}
		}
	}
	
	// 執行維護
	private static void exeDelOldTransaction(String central_no) {
		CallableStatement cstmt = null;
		Connection con = null;
		
		try {
			String sql = "{call " + ETL_Profile.db2TableSchema + ".TRANSACTION_CENTRAL_SERVICE.remove_OLD_Datas(?,?)}";

			con = ConnectionHelper.getDB2Connection(central_no);
			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.registerOutParameter(2, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			// 有錯誤釋出錯誤訊息 不往下繼續進行
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(2);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
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
			} catch (SQLException ex) {
				ex.printStackTrace();
				StringWriter sw = new StringWriter(); 
				PrintWriter pw = new PrintWriter(sw); 
				ex.printStackTrace(pw); 
				System.out.println("ExceptionMassage:" + sw.toString());
			}
		}
	}
	
	public static void main(String[] argvs) {
		
		System.out.println("test Start");
		
		
		try {
			// 放行參數 test
//			System.out.println(ETL_C_Comm.getGAMLDB_Profile(2));

			// 是否上班日判斷
//			System.out.println(ETL_C_Supervision.isBusinessDay(new Date()));
//			System.out.println(ETL_C_Supervision.isBusinessDay(new SimpleDateFormat("yyyyMMdd").parse("20190224")));
		
			// 前一個上班日
//			System.out.println(ETL_C_Master.getBeforeRecordDate(new Date()));
//			System.out.println(ETL_C_Master.getBeforeRecordDate(new SimpleDateFormat("yyyyMMdd").parse("20190224")));
			
			// 最新完成ETL日期資料
//			List<ETL_Bean_LogData> list = ETL_C_RelCheck_Notice.getRelCheckOver_CentralList(new Date());
//			for (int i = 0; i < list.size(); i++) {
//				System.out.print(list.get(i).getCENTRAL_NO());
//				System.out.print("  ");
//				System.out.println(list.get(i).getRECORD_DATE());
//			}
//			System.out.println("=========");
			
			// 判斷日期是否相等
			Date record_date = ETL_C_Master.getBeforeRecordDate(new SimpleDateFormat("yyyyMMdd").parse("20190215"));
//			System.out.println(record_date);
//			System.out.println("=========");
//			for (int i = 0; i < list.size(); i++) {
//				ETL_Bean_LogData aData = list.get(i);
//				// 若最近的資料日期未執行完畢, 則將該單位移除執行名單
//				if (record_date.compareTo(aData.getRECORD_DATE()) != 0) {
//					list.remove(i);
//					i--;
//				}
//			}
//			for (int i = 0; i < list.size(); i++) {
//				System.out.print(list.get(i).getCENTRAL_NO());
//				System.out.print("  ");
//				System.out.println(list.get(i).getRECORD_DATE());
//			}
			
			// 測試是否執行刪除舊交易資料
//			System.out.println(isExeDelOldTransaction(record_date));
			
			exeDelOldTransaction("018");
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		System.out.println("test End");
	}
	
}
