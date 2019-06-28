package Control;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Date;

import Bean.ETL_Bean_LogData;
import DB.ConnectionHelper;
import Profile.ETL_Profile;
import Tool.ETL_Tool_CastObjUtil;
import Tool.ETL_Tool_StringX;

public class ETL_C_Comm {
	
	// 擔保品, 保證人檔對party進行補漏作業
	public static void SupplementParty (ETL_Bean_LogData logData, String fedServer, String runTable) {
		
		System.out.println("#######ETL_C_Comm - SupplementParty - Start");
		
		try {
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Supplement.supplementParty(?,?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection(logData.getCENTRAL_NO().trim());
			CallableStatement cstmt = con.prepareCall(sql);
			
			Struct dataStruct = con.createStruct("T_LOGDATA", ETL_Tool_CastObjUtil.castObjectArr(logData));
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setObject(2, dataStruct);
			cstmt.setObject(3, fedServer);
			cstmt.setObject(4, runTable);
			cstmt.registerOutParameter(5, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(5);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
			StringWriter sw = new StringWriter(); 
			PrintWriter pw = new PrintWriter(sw); 
			ex.printStackTrace(pw); 
			System.out.println("ExceptionMassage:" + sw.toString());
		}
		
		System.out.println("#######ETL_C_Comm - SupplementParty - End");
		
	}
	
	// 修正來源保證人Branch Code
	public static void ModifyPartyBranchCodeFromAgent (ETL_Bean_LogData logData, String fedServer, String runTable) {
		
		System.out.println("#######ETL_C_Comm - ModifyPartyBranchCodeFromAgent - Start");
		
		try {
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Supplement.modifyPartyBranchCode(?,?,?,?,?)}";
			
			Connection con = ConnectionHelper.getDB2Connection(logData.getCENTRAL_NO().trim());
			CallableStatement cstmt = con.prepareCall(sql);
			
			Struct dataStruct = con.createStruct("T_LOGDATA", ETL_Tool_CastObjUtil.castObjectArr(logData));
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setObject(2, dataStruct);
			cstmt.setObject(3, fedServer);
			cstmt.setObject(4, runTable);
			cstmt.registerOutParameter(5, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(5);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
			StringWriter sw = new StringWriter(); 
			PrintWriter pw = new PrintWriter(sw); 
			ex.printStackTrace(pw); 
			System.out.println("ExceptionMassage:" + sw.toString());
		}
		
		System.out.println("#######ETL_C_Comm - ModifyPartyBranchCodeFromAgent - End");
		
	}
	
	// 取得正式機設定參數表內容
	public static String getGAMLDB_Profile(int p_No) {
		CallableStatement cstmt = null;
		java.sql.ResultSet rs = null;
		Connection con = null;
		String result = "";
		
		try {
			String sql = "{call " + ETL_Profile.db2TableSchema + ".GAMLDB.getProfile_StrValue(?,?,?,?)}";
			
			con = ConnectionHelper.getDB2Connection();
			cstmt = con.prepareCall(sql);
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setInt(2, p_No);
			cstmt.registerOutParameter(3, Types.VARCHAR);
			cstmt.registerOutParameter(4, Types.VARCHAR);
			
			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(4);
	            System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}
			
			result = cstmt.getString(3);
			
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
		
		return result;
	}

	public static void main(String[] args) {
		
		System.out.println("測試開始  Start " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
		try {
		
			ETL_Bean_LogData logData = new ETL_Bean_LogData();
			logData.setCENTRAL_NO("018");
			logData.setRECORD_DATE(ETL_Tool_StringX.toUtilDate("20190222"));
			
	//		SupplementParty(logData, null, "TEMP");
			ModifyPartyBranchCodeFromAgent(logData, "ETLDB001", "temp");
			
	//		String result = getGAMLDB_Profile(1);
	//		System.out.println("result = " + result);
		
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		System.out.println("測試結束  End " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));

	}

}
