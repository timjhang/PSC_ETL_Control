package Migration;

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

import Bean.ETL_Bean_DM_LogData;
import DB.ConnectionHelper;
import Profile.ETL_Profile;
import Tool.ETL_Tool_CastObjUtil;
import Tool.ETL_Tool_StringX;

public class ETL_DM_CHANGE_PartyID extends Migration {
	//ETL_DM_ALERT_ACCOUNT_BALANCE
	private String storeProcedureName;
	private String connectionDB;
	
	public ETL_DM_CHANGE_PartyID(ETL_Bean_DM_LogData logData,String connectionDB,String storeProcedureName) {
		super(logData);
		this.connectionDB = connectionDB;
		this.storeProcedureName = storeProcedureName;
	}

	@Override
	void migration_File() {

		System.out.println("#######Migration - " + storeProcedureName + "- Start");

		Connection con = null;
		CallableStatement cstmt = null;
		try {
			String sql = "{call " + ETL_Profile.db2TableSchema + ".Migration_PartyID." + storeProcedureName + "(?,?,?)}";
			con = ConnectionHelper.getDB2Connection(connectionDB);
			cstmt = con.prepareCall(sql);
			Struct dataStruct = con.createStruct("T_ETL_FILE_LOG", ETL_Tool_CastObjUtil.castObjectArr(logData));
			cstmt.registerOutParameter(1, Types.INTEGER);
			cstmt.setObject(2, dataStruct);
			cstmt.registerOutParameter(3, Types.VARCHAR);

			cstmt.execute();
			int returnCode = cstmt.getInt(1);
			if (returnCode != 0) {
				String errorMessage = cstmt.getString(3);
				System.out.println("Error Code = " + returnCode + ", Error Message : " + errorMessage + "storeProcedureName:" + storeProcedureName);
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

		System.out.println("#######Migration - " + storeProcedureName + "- END");
	}
	

	/**
	 * @param logData
	 * logData 請至少裝入
	 * 		Batch_no
	 * 		Central_no
	 * 		Record_date
	 * 		step_type
	 * 
	 * @return
	 */
	public static List<Migration> getMigrationByChangesPartyID (ETL_Bean_DM_LogData logData){
		
		List<Migration> migrations = new ArrayList<Migration>();
		
		// clone 新class指標用
		ETL_Bean_DM_LogData logData2 = new ETL_Bean_DM_LogData();
		
		// 28支轉換程式
		String [] storeProcedureNames = {
				"trans_SRC_TRANSACTION"
				,"trans_SRC_PARTY_RISK_RANK"
				,"trans_SRC_INDICATION"
				,"trans_SRC_TRANSFER"
				,"trans_SRC_SERVICE"
				,"trans_SRC_FEEDBACK"
				,"trans_SRC_PARTY_GROUP"
				,"trans_SRC_PARTY_GROUP_MEMBER"
				,"Migration_PartyID_ALERT_ADDRESS"
				,"Migration_PartyID_ALERT_LOAN"
				,"Migration_PartyID_ALERT_LOAN_MASTER"
				,"Migration_PartyID_ALERT_PARTY"
				,"Migration_PartyID_ALERT_PARTY_ADDRESS"
				,"Migration_PartyID_ALERT_PARTY_EMAIL"
				,"Migration_PartyID_ALERT_PARTY_GROUP"
				,"Migration_PartyID_ALERT_PARTY_GROUP_MEMBER"
				,"Migration_PartyID_ALERT_PARTY_ID"
				,"Migration_PartyID_ALERT_PARTY_PARTY_REL"
				,"Migration_PartyID_ALERT_PARTY_PHONE"
				,"Migration_PartyID_ALERT_SERVICE"
				,"Migration_PartyID_ALERTS"
				,"Migration_PartyID_KYC_BENEFICIARIES_INFO"
				,"Migration_PartyID_KYC_PARTY"
				,"Migration_PartyID_KYCS"
				,"Migration_PartyID_REVIEWS"
				,"Migration_PartyID_USR_WHITE_LIST_ENTRY"
				,"Migration_PartyID_MANAGED_TXN"
				,"Migration_PartyID_KYC_SENIOR_OFFICER_INFO"
				,"Migration_PartyID_ALERT_TRANSFER"
		};

		
		for (String storeProcedureName : storeProcedureNames) {
			// 執行25支DM id轉換系列程式
			logData2 = logData.clone();
			logData2.setFile_name(storeProcedureName);
			
			if(storeProcedureName.substring(0, 5).equals("trans")) {
				migrations.add(new ETL_DM_CHANGE_PartyID(logData2,logData2.getCentral_no(),logData2.getFile_name()));
			}else {
				migrations.add(new ETL_DM_CHANGE_PartyID(logData2, "DB", logData2.getFile_name()));
			}
		}
		return migrations;
	}
	
	public static void callDMfunctionPartyID(ETL_Bean_DM_LogData logData) {
		
		System.out.println("#### ETL_DM_CHANGE_PartyID callDMfunctionPartyID - Start  " +  new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		
		logData.setStep_type("M");
		
		List<Migration> migrations = ETL_DM_CHANGE_PartyID.getMigrationByChangesPartyID(logData);

		ExecutorService executor = Executors.newFixedThreadPool(5);

		for (Migration migration : migrations) {
			executor.execute(migration);
		}
		
		executor.shutdown();

		while (!executor.isTerminated()) {

		}
		
		System.out.println("線程池已經關閉");
		
		System.out.println("#### ETL_DM_CHANGE_PartyID callDMfunctionPartyID - End  " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
	}

	public static void main(String[] args) throws Exception {

		
//		System.out.println(ETL_Tool_StringX.toUtilDate("2018-08-17"));
//
//		String record_date_str = "", central_no = "", batch_no = "";
//
//		System.out.print("請輸入批號: ");
//		Scanner scanner = new Scanner(System.in);
//		batch_no = scanner.next();
//
//		System.out.print("請輸入日期(格式yyyyMMdd): ");
//		scanner = new Scanner(System.in);
//		record_date_str = scanner.next();
//
//		System.out.print("請輸入單位: ");
//		scanner = new Scanner(System.in);
//		central_no = scanner.next();
		
		// clone 新class指標用
//		ETL_Bean_DM_LogData logData = new ETL_Bean_DM_LogData();
//		logData.setBatch_no(batch_no);
//		logData.setCentral_no(central_no);
//		logData.setRecord_date(Tool.ETL_Tool_StringX.toUtilDate(record_date_str));
//		logData.setStep_type("M");
		
		
		System.out.println("執行開始");
		
		ETL_Bean_DM_LogData newOne = new ETL_Bean_DM_LogData();
		newOne.setBatch_no("MIG00075");
		newOne.setCentral_no("600");
		Date record_date = new SimpleDateFormat("yyyyMMdd").parse("20180817");
		newOne.setRecord_date(record_date);
		newOne.setUpload_no("001");
		newOne.setStep_type("M");

		List<Migration> migrations = ETL_DM_CHANGE_PartyID.getMigrationByChangesPartyID(newOne);

		ExecutorService executor = Executors.newFixedThreadPool(5);

		for (Migration migration : migrations) {
			executor.execute(migration);
		}
		
		executor.shutdown();

		while (!executor.isTerminated()) {
		}
		System.out.println("線程池已經關閉");
		
		System.out.println("執行結束");
	}
	

}
