package DB;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import Profile.ETL_Profile;

public class ETL_P_Log {

	public static void main(String[] args) throws Exception {
//		String CENTRAL_NO = "951";
//		DateFormat formatter = new SimpleDateFormat("yyyyMMdd");
//		java.util.Date date = new java.util.Date();
//		java.util.Date RECORD_DATE = formatter.parse(formatter.format(date));
//		String FILE_TYPE = "CF";
//		String FILE_NAME = "PARTY";
//		String UPLOAD_NO = "001";
//		String STEP_TYPE = "E";
//		formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
//		java.util.Date START_DATETIME = formatter.parse(formatter.format(date));
//		java.util.Date END_DATETIME = formatter.parse(formatter.format(date));
//		int TOTAL_CNT = 5;
//		int SUCCESS_CNT = 5;
//		int FAILED_CNT = 0;
//		String SRC_FILE = "951_CF_PARTY_20171211.TXT";
//		ETL_P_Log.write_ETL_Log(CENTRAL_NO, RECORD_DATE, FILE_TYPE, FILE_NAME, UPLOAD_NO, STEP_TYPE, START_DATETIME,
//				END_DATETIME, TOTAL_CNT, SUCCESS_CNT, FAILED_CNT, SRC_FILE);
//		
//		write_ETL_Detail_Log("123", "234", new java.util.Date(), "456", "567", 
//				"678", "S", "", "", new java.util.Date(),
//				null);
//		write_ETL_Detail_Log("555", "234", new java.util.Date(), "456", "5", 
//				"6", "S", "", "", null,
//				null);
//		
//		update_ETL_Detail_Log("555", "234", new java.util.Date(), "456", "5", 
//				"6", "S", "", "", new java.util.Date());
		
	}

	/**
	 * ETL_FILE_Log格式
	 * @param BATCH_NO 批次編號
	 * @param CENTRAL_NO 報送單位
	 * @param RECORD_DATE 檔案日期
	 * @param FILE_TYPE 檔名業務別
	 * @param FILE_NAME 檔案名稱
	 * @param UPLOAD_NO 上傳批號
	 * @param STEP_TYPE 步驟
	 * @param START_DATETIME 執行開始日期時間
	 * @param END_DATETIME 執行結束日期時間
	 * @param TOTAL_CNT 總筆數
	 * @param SUCCESS_CNT 成功筆數
	 * @param FAILED_CNT 失敗筆數
	 * @param SRC_FILE 來源檔案
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public static void write_ETL_FILE_Log(String BATCH_NO, String CENTRAL_NO, java.util.Date RECORD_DATE, String FILE_TYPE, String FILE_NAME,
			String UPLOAD_NO, String STEP_TYPE, java.util.Date START_DATETIME, java.util.Date END_DATETIME,
			int TOTAL_CNT, int SUCCESS_CNT, int FAILED_CNT, String SRC_FILE)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {

		String sql_statement = 
				" INSERT INTO " + ETL_Profile.db2TableSchema + ".ETL_FILE_LOG ( " +
					" BATCH_NO, " +
					" CENTRAL_NO, " +
					" RECORD_DATE, " +
					" FILE_TYPE, " +
					" FILE_NAME, " +
					" UPLOAD_NO, " +
					" STEP_TYPE, " +
					" START_DATETIME, " +
					" END_DATETIME, " +
					" TOTAL_CNT, " +
					" SUCCESS_CNT, " +
					" FAILED_CNT, " +
					" EXE_RESULT, " +
					" EXE_RESULT_DESCRIPTION, " +
					" SRC_FILE " +
				") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		Connection con = ConnectionHelper.getDB2Connection();
		PreparedStatement pstmt = con.prepareStatement(sql_statement);

		pstmt.setString(1, BATCH_NO);
		pstmt.setString(2, CENTRAL_NO);
		pstmt.setDate(3, new Date(RECORD_DATE.getTime()));
		pstmt.setString(4, FILE_TYPE);
		pstmt.setString(5, FILE_NAME);
		pstmt.setString(6, UPLOAD_NO);
		pstmt.setString(7, STEP_TYPE);
		pstmt.setTimestamp(8, (START_DATETIME==null)?null:(new Timestamp(START_DATETIME.getTime())));
		pstmt.setTimestamp(9, (END_DATETIME==null)?null:(new Timestamp(END_DATETIME.getTime())));
		pstmt.setInt(10, TOTAL_CNT);
		pstmt.setInt(11, SUCCESS_CNT);
		pstmt.setInt(12, FAILED_CNT);
		pstmt.setString(13, "");
		pstmt.setString(14, "");
		pstmt.setString(15, SRC_FILE);

		pstmt.executeUpdate();

		if (pstmt != null) {
			pstmt.close();
		}
		if (con != null) {
			con.close();
		}

	}
	
	/**  更新ETL_FILE_Log  V3  2018.01.24  TimJhang
	 * ETL_FILE_Log格式
	 * @param BATCH_NO 批次編號
	 * @param CENTRAL_NO 報送單位
	 * @param RECORD_DATE 檔案日期
	 * @param FILE_TYPE 檔名業務別
	 * @param FILE_NAME 檔案名稱
	 * @param UPLOAD_NO 上傳批號
	 * @param STEP_TYPE 步驟 
	 * @param END_DATETIME 執行結束日期時間
	 * @param TOTAL_CNT 總筆數
	 * @param SUCCESS_CNT 成功筆數
	 * @param FAILED_CNT 失敗筆數
	 * @param EXE_RESULT 執行結果
	 * @param EXE_RESULT_DESCRIPTION 執行結果說明
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public static void update_End_ETL_FILE_Log(String BATCH_NO, String CENTRAL_NO, java.util.Date RECORD_DATE, String FILE_TYPE, String FILE_NAME,
			String UPLOAD_NO, String STEP_TYPE, java.util.Date END_DATETIME,
			int TOTAL_CNT, int SUCCESS_CNT, int FAILED_CNT,
			String EXE_RESULT, String EXE_RESULT_DESCRIPTION)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {

		String sql_statement = 
				" UPDATE " + ETL_Profile.db2TableSchema + ".ETL_FILE_LOG SET " +
					" END_DATETIME = ?, " +
					" TOTAL_CNT = ?, " +
					" SUCCESS_CNT = ?, " +
					" FAILED_CNT = ?, " +
					" EXE_RESULT = ?, " +
					" EXE_RESULT_DESCRIPTION = ? " +
				" WHERE BATCH_NO = ? " + 
					" AND CENTRAL_NO = ? " +
					" AND RECORD_DATE = ? " +
					" AND FILE_TYPE = ? " +
					" AND FILE_NAME = ? " +
					" AND UPLOAD_NO = ? " +
					" AND STEP_TYPE = ? "
				;

		Connection con = ConnectionHelper.getDB2Connection();
		PreparedStatement pstmt = con.prepareStatement(sql_statement);

		pstmt.setTimestamp(1, new Timestamp(END_DATETIME.getTime()));
		pstmt.setInt(2, TOTAL_CNT);
		pstmt.setInt(3, SUCCESS_CNT);
		pstmt.setInt(4, FAILED_CNT);
		pstmt.setString(5, EXE_RESULT);
		pstmt.setString(6, EXE_RESULT_DESCRIPTION);
		pstmt.setString(7, BATCH_NO);
		pstmt.setString(8, CENTRAL_NO);
		pstmt.setDate(9, new Date(RECORD_DATE.getTime()));
		pstmt.setString(10, FILE_TYPE);
		pstmt.setString(11, FILE_NAME);
		pstmt.setString(12, UPLOAD_NO);
		pstmt.setString(13, STEP_TYPE);
		

		pstmt.executeUpdate();

		if (pstmt != null) {
			pstmt.close();
		}
		if (con != null) {
			con.close();
		}

	}

	/**  未更新V2  2017.12.29  Tim Jhang
	 * Error_Log格式
	 * @param CENTRAL_NO 報送單位
	 * @param RECORD_DATE 檔案日期
	 * @param FILE_TYPE 檔名業務別
	 * @param FILE_NAME 檔案名稱
	 * @param UPLOAD_NO 上傳批號
	 * @param STEP_TYPE 步驟
	 * @param ROW_COUNT 行數
	 * @param FIELD_NAME 欄位中文名稱
	 * @param ERROR_DESCRIPTION 錯誤描述
	 * @param SRC_FILE 來源檔案
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public static void write_Error_Log(String BATCH_NO, String CENTRAL_NO, java.util.Date RECORD_DATE, String FILE_TYPE,
			String FILE_NAME, String UPLOAD_NO, String STEP_TYPE, String ROW_COUNT, String FIELD_NAME,
			String ERROR_DESCRIPTION, String SRC_FILE) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {

		String sql_statement = " INSERT INTO " + ETL_Profile.db2TableSchema + ".Error_Log (" +
				"BATCH_NO," +
				"CENTRAL_NO," +
				"RECORD_DATE," +
				"FILE_TYPE," +
				"FILE_NAME," +
				"UPLOAD_NO," +
				"STEP_TYPE," +
				"ROW_COUNT," +
				"FIELD_NAME," +
				"ERROR_DESCRIPTION," +
				"SRC_FILE" +
				") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

		Connection con = ConnectionHelper.getDB2Connection();
		PreparedStatement pstmt = con.prepareStatement(sql_statement);

		pstmt.setString(1, BATCH_NO);
		pstmt.setString(2, CENTRAL_NO);
		pstmt.setDate(3, new Date(RECORD_DATE.getTime()));
		pstmt.setString(4, FILE_TYPE);
		pstmt.setString(5, FILE_NAME);
		pstmt.setString(6, UPLOAD_NO);
		pstmt.setString(7, STEP_TYPE);
		pstmt.setString(8, ROW_COUNT);
		pstmt.setString(9, FIELD_NAME);
		pstmt.setString(10, ERROR_DESCRIPTION);
		pstmt.setString(11, SRC_FILE);

		pstmt.executeUpdate();

		if (pstmt != null) {
			pstmt.close();
		}
		if (con != null) {
			con.close();
		}
	}
	
	// 寫入ETL_Detail_Log
	/**
	 * 
	 * @param batch_no 批次編號
	 * @param central_no 報送單位
	 * @param record_date 檔案日期
	 * @param upload_no 上傳批號
	 * @param step_type 步驟
	 * @param program_no 程式代號
	 * @param exe_status 執行狀態
	 * @param exe_result 執行結果
	 * @param exe_result_description 執行結果說明
	 * @param start_datetime 開始日期時間
	 * @param end_datetime 結束日期時間
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public static void write_ETL_Detail_Log(
			String batch_no, String central_no, java.util.Date record_date, String upload_no, String step_type, 
			String program_no, String exe_status, String exe_result, String exe_result_description, java.util.Date start_datetime,
			java.util.Date end_datetime) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		
		String sql_statement = 
				" INSERT INTO " + ETL_Profile.db2TableSchema + ".ETL_DETAIL_LOG ( " +
					" BATCH_NO, " +
					" CENTRAL_NO, " +
					" RECORD_DATE, " +
					" UPLOAD_NO, " +
					" STEP_TYPE, " +
					" PROGRAM_NO, " +
					" EXE_STATUS, " +
					" EXE_RESULT, " +
					" EXE_RESULT_DESCRIPTION, " +
					" START_DATETIME, " +
					" END_DATETIME " +
				" ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";

		Connection con = ConnectionHelper.getDB2Connection();
		PreparedStatement pstmt = con.prepareStatement(sql_statement);

		pstmt.setString(1, batch_no);
		pstmt.setString(2, central_no);
		pstmt.setDate(3, (record_date==null)?null:(new java.sql.Date(record_date.getTime())));
		pstmt.setString(4, upload_no);
		pstmt.setString(5, step_type);
		pstmt.setString(6, program_no);
		pstmt.setString(7, exe_status);
		pstmt.setString(8, exe_result);
		pstmt.setString(9, exe_result_description);
		pstmt.setTimestamp(10, (start_datetime==null)?null:(new Timestamp(start_datetime.getTime())));
		pstmt.setTimestamp(11, (end_datetime==null)?null:(new Timestamp(end_datetime.getTime())));
		
		pstmt.executeUpdate();

		if (pstmt != null) {
			pstmt.close();
		}
		if (con != null) {
			con.close();
		}
	}
	
	// 更新ETL_Detail_Log
	/**
	 * 
	 * @param batch_no 批次編號(條件)
	 * @param central_no 報送單位(條件)
	 * @param record_date 檔案日期(條件)
	 * @param upload_no 上傳批號(條件)
	 * @param step_type 步驟(條件)
	 * @param program_no 程式代號(更新)
	 * @param exe_status 執行狀態(更新)
	 * @param exe_result 執行結果(更新)
	 * @param exe_result_description 執行結果說明(更新)
	 * @param end_datetime 結束日期時間(更新)
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public static void update_End_ETL_Detail_Log (
			// 搜尋項目參數
			String batch_no, String central_no, java.util.Date record_date, String upload_no, String step_type,
			// 更新參數
			String program_no, String exe_status, String exe_result, String exe_result_description, java.util.Date end_datetime
			) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		
		String sql_statement = " UPDATE " + ETL_Profile.db2TableSchema + ".ETL_DETAIL_LOG " +
				" SET " +
					" EXE_STATUS = ? ," +
					" EXE_RESULT = ? ," +
					" EXE_RESULT_DESCRIPTION = ? ," +
					" END_DATETIME = ? " +
				" WHERE " +
					" BATCH_NO = ? " +
					" AND CENTRAL_NO = ? " +
					" AND RECORD_DATE = ? " +
					" AND UPLOAD_NO = ? " +
					" AND STEP_TYPE = ? " +
					" AND PROGRAM_NO = ? ";

		Connection con = ConnectionHelper.getDB2Connection();
		PreparedStatement pstmt = con.prepareStatement(sql_statement);

		
		pstmt.setString(1, exe_status);
		pstmt.setString(2, exe_result);
		pstmt.setString(3, exe_result_description);
		pstmt.setTimestamp(4, (end_datetime==null)?null:(new Timestamp(end_datetime.getTime())));
		pstmt.setString(5, batch_no);
		pstmt.setString(6, central_no);
		pstmt.setDate(7, (record_date==null)?null:(new java.sql.Date(record_date.getTime())));
		pstmt.setString(8, upload_no);
		pstmt.setString(9, step_type);
		pstmt.setString(10, program_no);

		pstmt.executeUpdate();

		if (pstmt != null) {
			pstmt.close();
		}
		if (con != null) {
			con.close();
		}
	}
	
	// 查詢ETL_Detail_Log  是否有寫入紀錄
	/**
	 * 
	 * @param batch_no 批次編號(條件)
	 * @param central_no 報送單位(條件)
	 * @param record_date 檔案日期(條件)
	 * @param upload_no 上傳批號(條件)
	 * @param step_type 步驟(條件)
	 * @param program_no 程式代號(更新)
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public static boolean query_ETL_Detail_Log_Done (
			// 搜尋項目參數
			String batch_no, String central_no, java.util.Date record_date, String upload_no, String step_type, String program_no
			) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		
		int resultCount = 0;
		
		String sql_statement = 
				" SELECT COUNT(*) FROM  " + ETL_Profile.db2TableSchema + ".ETL_DETAIL_LOG " +
				" WHERE " +
					" BATCH_NO = ? " +
					" AND CENTRAL_NO = ? " +
					" AND RECORD_DATE = ? " +
					" AND UPLOAD_NO = ? " +
					" AND STEP_TYPE = ? " +
					" AND PROGRAM_NO = ? ";

		Connection con = ConnectionHelper.getDB2Connection();
		PreparedStatement pstmt = con.prepareStatement(sql_statement);

		pstmt.setString(1, batch_no);
		pstmt.setString(2, central_no);
		pstmt.setDate(3, (record_date==null)?null:(new java.sql.Date(record_date.getTime())));
		pstmt.setString(4, upload_no);
		pstmt.setString(5, step_type);
		pstmt.setString(6, program_no);

		java.sql.ResultSet rs = pstmt.executeQuery();
		
		if (rs.next()) {
			resultCount = rs.getInt(1);
		}

		if (pstmt != null) {
			pstmt.close();
		}
		if (con != null) {
			con.close();
		}
		
		if (resultCount > 0) {
			// 若有紀錄則回傳true
			return true;
		} else {
			// 若無紀錄則回傳false
			return false;
		}
		
	}

}
