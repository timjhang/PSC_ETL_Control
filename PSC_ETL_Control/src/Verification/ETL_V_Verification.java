package Verification;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Struct;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import Bean.ETL_Bean_LogData;
import DB.ConnectionHelper;
import Profile.ETL_Profile;
import Tool.ETL_Tool_CastObjUtil;

public class ETL_V_Verification extends Verification {

	private String storeProcedureName;
	
	public ETL_V_Verification(ETL_Bean_LogData logData, String storeProcedureName) {
		super(logData);
		this.storeProcedureName = storeProcedureName;
	}

	@Override
	void verification_Datas() {
		System.out.println("#######Verification - " + storeProcedureName + " - Start");

		Connection con = null;
		CallableStatement cstmt = null;
		
		try {
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Verification." + storeProcedureName + "(?,?,?)}";
			
			con = ConnectionHelper.getDB2Connection(logData.getCENTRAL_NO());
			cstmt = con.prepareCall(sql);
			
			Struct dataStruct = con.createStruct("T_LOGDATA", ETL_Tool_CastObjUtil.castObjectArr(logData));
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setObject(2, dataStruct);
			cstmt.registerOutParameter(3, Types.VARCHAR);

			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(3);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage + ", storeProcedureName : " + storeProcedureName);
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				// 資源後開,先關
				if (cstmt != null) {
					cstmt.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}

		System.out.println("#######Verification - " + storeProcedureName + " - End");
	}
	
	public static void verification_log_delete(ETL_Bean_LogData logData) {
		System.out.println("#######Verification - verification_log_delete - Start");

		Connection con = null;
		CallableStatement cstmt = null;
		
		try {
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Verification.INSERT_L_VERIFICATION_LOG_Delete(?,?,?)}";
			
			con = ConnectionHelper.getDB2Connection(logData.getCENTRAL_NO());
			cstmt = con.prepareCall(sql);
			
			Struct dataStruct = con.createStruct("T_LOGDATA", ETL_Tool_CastObjUtil.castObjectArr(logData));
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setObject(2, dataStruct);
			cstmt.registerOutParameter(3, Types.VARCHAR);

			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(3);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage + ", storeProcedureName : insert_VERIFICATION_LOG_BY_MERGE");
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				// 資源後開,先關
				if (cstmt != null) {
					cstmt.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}

		System.out.println("#######Verification - verification_log_delete - End");
	}
	
	public static void verification_log_by_merge(ETL_Bean_LogData logData) {
		System.out.println("#######Verification - insert_VERIFICATION_LOG_BY_MERGE - Start");

		Connection con = null;
		CallableStatement cstmt = null;
		
		try {
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Verification.insert_VERIFICATION_LOG_BY_MERGE(?,?,?)}";
			
			con = ConnectionHelper.getDB2Connection(logData.getCENTRAL_NO());
			cstmt = con.prepareCall(sql);
			
			Struct dataStruct = con.createStruct("T_LOGDATA", ETL_Tool_CastObjUtil.castObjectArr(logData));
			
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setObject(2, dataStruct);
			cstmt.registerOutParameter(3, Types.VARCHAR);

			cstmt.execute();
			
			int returnCode = cstmt.getInt(1);
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(3);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage + ", storeProcedureName : insert_VERIFICATION_LOG_BY_MERGE");
			}
			
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				// 資源後開,先關
				if (cstmt != null) {
					cstmt.close();
				}
				if (con != null) {
					con.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}

		System.out.println("#######Verification - insert_VERIFICATION_LOG_BY_MERGE - End");
	}
	
	
	private static List<Verification> getVerificationByL(ETL_Bean_LogData logData) {
		
		List<Verification> verifications = new ArrayList<Verification>();
		
		// clone 新class指標用
		ETL_Bean_LogData logData2 = new ETL_Bean_LogData();
		
		String [] storeProcedureNames = {
			"insert_L_VERIFICATION_LOG_BY_PARTY",
			"INSERT_L_VERIFICATION_LOG_BY_PARTY_PARTY_REL",
			"INSERT_L_VERIFICATION_LOG_BY_PARTY_PHONE",
			"INSERT_L_VERIFICATION_LOG_BY_PARTY_ADDRESS",
			"INSERT_L_VERIFICATION_LOG_BY_PARTY_EMAIL",
			"INSERT_L_VERIFICATION_LOG_BY_PARTY_NATIONALITY",
			"INSERT_L_VERIFICATION_LOG_BY_ACCOUNT",
			"INSERT_L_VERIFICATION_LOG_BY_ACCOUNT_PROPERTY",
			"INSERT_L_VERIFICATION_LOG_BY_PARTY_ACCOUNT_REL",
			"INSERT_L_VERIFICATION_LOG_BY_BALANCE",
			"INSERT_L_VERIFICATION_LOG_BY_TRANSACTION",
			"INSERT_L_VERIFICATION_LOG_BY_TRANSFER",
			"INSERT_L_VERIFICATION_LOG_BY_SERVICE",
			"INSERT_L_VERIFICATION_LOG_BY_LOAN_MASTER",
			"INSERT_L_VERIFICATION_LOG_BY_LOAN_DETAIL",
			"INSERT_L_VERIFICATION_LOG_BY_LOAN",
			"INSERT_L_VERIFICATION_LOG_BY_LOAN_COLLATERAL",
			"INSERT_L_VERIFICATION_LOG_BY_LOAN_GUARANTOR",
			"INSERT_L_VERIFICATION_LOG_BY_FX_RATE"
		};
		
		for (String storeProcedureName : storeProcedureNames) {
			logData2 = logData.clone();
			verifications.add(new ETL_V_Verification(logData2, storeProcedureName));
		}
		
		return verifications;
	}
	
	public static void callVerificationFunctionByL(ETL_Bean_LogData logData) {
		System.out.println("#### ETL_V_Verification callVerificationFunctionByL - Start  " +  new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
		try {
			List<Verification> verifications = getVerificationByL(logData);
			
			ExecutorService executor = Executors.newFixedThreadPool(5);
	
			for (Verification verification : verifications) {
				executor.execute(verification);
			}
			
			executor.shutdown();
	
			while (!executor.isTerminated()) {
			}
			
			System.out.println("線程池已經關閉");
			
			verification_log_delete(logData);
			
			verification_log_by_merge(logData);
		
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		System.out.println("#### ETL_V_Verification callVerificationFunctionByL - End  " +  new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
	}
}