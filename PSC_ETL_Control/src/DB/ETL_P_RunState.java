package DB;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import Bean.ETL_Bean_LogData;
import Profile.ETL_Profile;
import Tool.ETL_Tool_StringX;
import Tool.ETL_Tool_Thread;

public class ETL_P_RunState extends ETL_Tool_Thread {
	private String tableName;

	public ETL_P_RunState(ETL_Bean_LogData logData, String tableName) {
		super(logData);
		this.tableName = tableName;
	}

	@Override
	protected void thread_work() {
		Date date = new Date();
		System.out.println("執行 runStateSRC " + logData.getCENTRAL_NO() + "  tableName:" + tableName + " Start:" + ETL_Tool_StringX.toUtilDateStr(date, "yyyy-MM-dd hh:mm:ss"));

		Connection con = null;
		CallableStatement cstmt = null;
		try {
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Load.runStateSRC_Parallel(?,?,?)}";

			con = ConnectionHelper.getDB2Connection(logData.getCENTRAL_NO());
			cstmt = con.prepareCall(sql);

			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setString(2, tableName);
			cstmt.registerOutParameter(3, Types.VARCHAR);

			cstmt.execute();

			int returnCode = cstmt.getInt(1);

			// 有錯誤釋出錯誤訊息
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(3);
				System.out.println("####runStateSRC - Error Code = " + returnCode + ", Error Message : " + errorMessage);
				ETL_P_Log.write_Runtime_Log("runStateSRC", "####runStateSRC - Error Code = " + returnCode + ", Error Message : " + errorMessage);
			}

		} catch (Exception ex) {
			ex.printStackTrace();
			ETL_P_Log.write_Runtime_Log("runStateSRC", ex.getMessage());
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
			date = new Date();
			System.out.println("結束 runStateSRC " + logData.getCENTRAL_NO() + "  tableName:" + tableName + " END:" + ETL_Tool_StringX.toUtilDateStr(date, "yyyy-MM-dd hh:mm:ss"));
		}

	}
	
	
	/**
	 * @param logData
	 * logData 請至少裝入
	 * 		Central_no
	 * 
	 * @return
	 */
	private static List<ETL_P_RunState> getETL_P_RunState(ETL_Bean_LogData logData) {
		

		List<ETL_P_RunState> runStates = new ArrayList<ETL_P_RunState>();
		// clone 新class指標用
		ETL_Bean_LogData logData2 = new ETL_Bean_LogData();
		
		logData2 = logData.clone();
		
		String[] tableNames = {
				"PARTY",
				"PARTY_PARTY_REL",
				"PARTY_PHONE",
				"PARTY_ADDRESS",
				"PARTY_EMAIL",
				"ACCOUNT",
				"ACCOUNT_PROPERTY",
				"PARTY_ACCOUNT_REL",
				"BALANCE",
				"LOAN_MASTER",
				"LOAN_DETAIL",
				"LOAN",
				"LOAN_COLLATERAL",
				"LOAN_GUARANTOR",
				"FX_RATE",
				"CALENDAR",
				"TRANSACTION",
				"TRANSFER",
				"SERVICE"};

		for (String tableName : tableNames) {
			runStates.add(new ETL_P_RunState(logData2,tableName));
		}
		return runStates;
	}
	
	/**
	 * 
	 * @param central_no
	 */
	public static void runStateStart (String central_no) {
		ETL_Bean_LogData logData = new ETL_Bean_LogData();
		logData.setBATCH_NO("runState");
		logData.setCENTRAL_NO(central_no);
		Date date = new Date();
		
		System.out.println("執行 runStateSRC_Master " + logData.getCENTRAL_NO() + " Start:" + ETL_Tool_StringX.toUtilDateStr(date, "yyyy-MM-dd hh:mm:ss"));
		List<ETL_P_RunState> runStates = getETL_P_RunState(logData);
		ExecutorService executor = Executors.newFixedThreadPool(20);
		for (ETL_P_RunState runState : runStates) {
			executor.execute(runState);
		}

		executor.shutdown();

		while (!executor.isTerminated()) {
		}
		 date = new Date();
		System.out.println("結束 runStateSRC_Master " + logData.getCENTRAL_NO() + " END:" + ETL_Tool_StringX.toUtilDateStr(date, "yyyy-MM-dd hh:mm:ss"));
	
		System.out.println("線程池已經關閉");
		
	}
	
	public static void main(String args[]) {
		runStateStart ("600");
	}

}
