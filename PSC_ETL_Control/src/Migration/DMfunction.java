package Migration;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import Bean.ETL_Bean_DM_LogData;

public class DMfunction {
	
//	// 執行SRC Loan Domain Migration程式
//	public static void callDMfuncionLOAN(ETL_Bean_DM_LogData logData) {
//		try {
//		
//			// clone 新class指標用
//			ETL_Bean_DM_LogData logData2 = logData.clone();
//			new ETL_DM_LOAN_TR(logData2).migration_File();
//			
//		} catch (Exception ex) {
//			System.out.println("執行ETL_DM_LOAN_TR發生錯誤:" + ex.getMessage());
//		}
//		
//	}
	
	// 執行DMfunction 中的TR_Mapping落第
	public static void callDMfuncionTR_MAPPING(ETL_Bean_DM_LogData logData) {
		try {
		
			// clone 新class指標用
			ETL_Bean_DM_LogData logData2 = logData.clone();
			new ETL_DM_TR_MAPPING(logData2).migration_File();
			
		} catch (Exception ex) {
			System.out.println("執行ETL_DM_TR_MAPPING發生錯誤:" + ex.getMessage());
		}
		
	}
	
	// 執行DMfunction 中的KYCS記錄檔
	public static void callDMfuncionKYCS(ETL_Bean_DM_LogData logData) {
		try {
		
			// clone 新class指標用
			ETL_Bean_DM_LogData logData2 = logData.clone();
			new ETL_DM_KYCS(logData2).migration_File();
			
		} catch (Exception ex) {
			System.out.println("執行ETL_DM_KYCS發生錯誤:" + ex.getMessage());
		}
		
	}
	
//	// 執行SRC Domain Migration程式
//	public static void callDMfunctionETL(ETL_Bean_DM_LogData logData) {
//		// clone 新class指標用
//		ETL_Bean_DM_LogData logData2;
//
//		List<Migration> migrations = new ArrayList<Migration>();
//
//		String stepStr = "";
//
//		System.out.println("#### DMfunctionETL Start " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
//
//		try {
//
//			// 執行6支DM系列程式
//			logData2 = logData.clone();
//			migrations.add(new ETL_DM_PARTY_RISK_RANK_TR(logData2));
//
//			logData2 = logData.clone();
//			migrations.add(new ETL_DM_FEEDBACK_TR(logData2));
//
//			logData2 = logData.clone();
//			migrations.add(new ETL_DM_INDICATION_TR(logData2));
//
//			logData2 = logData.clone();
//			migrations.add(new ETL_DM_TRANSACTION_TR(logData2));
//
//			logData2 = logData.clone();
//			migrations.add(new ETL_DM_TRANSFER_TR(logData2));
//
//			logData2 = logData.clone();
//			migrations.add(new ETL_DM_SERVICE_TR(logData2));
//
////			logData2 = logData.clone();
////			migrations.add(new ETL_DM_LOAN_TR(logData2));
//
//			stepStr = "DM多線程";
//			ExecutorService executor = Executors.newFixedThreadPool(3);
//
//			for (Migration migration : migrations) {
//				executor.execute(migration);
//			}
//
//			executor.shutdown();
//
//			while (!executor.isTerminated()) {
//			}
//
//			System.out.println("線程池已經關閉");
//			
//		} catch (Exception ex) {
//			ex.printStackTrace();
//			System.out.println("執行" + stepStr + "發生錯誤:" + ex.getMessage());
//		}
//
//		System.out.println("#### DMfunctionETL End " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
//	}
	
	// 執行CMT Domain Migration程式
//	public static void callDMfunctionCMT(ETL_Bean_DM_LogData logData) {
	public static void callDMfunctionDomain(ETL_Bean_DM_LogData logData) {

		// clone 新class指標用
		ETL_Bean_DM_LogData logData2;

		List<Migration> migrations = new ArrayList<Migration>();

		String stepStr = "";

//		System.out.println("#### DMfunctionCMT Start " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		System.out.println("#### DMfunctionDomain Start " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));

		try {

			// 執行1 + 6 + 38 支DM系列程式
			logData2 = logData.clone();
			migrations.add(new ETL_DM_LOAN_TR(logData2));
			
			////
			
			logData2 = logData.clone();
			migrations.add(new ETL_DM_PARTY_RISK_RANK_TR(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_FEEDBACK_TR(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_INDICATION_TR(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_TRANSACTION_TR(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_TRANSFER_TR(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_SERVICE_TR(logData2));
			
			////
			
			logData2 = logData.clone();
			migrations.add(new ETL_DM_DOMAIN_CENTER_MAPPING(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_BRANCH_ID(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_USERS(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_MANAGED_TXN(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_USR_WHITE_LIST_ENTRY(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_SUPPRESS_NAME(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_REVIEWS(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_QUESTIONS(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_QUESTION_TEMPLATE(logData2));

//			logData2 = logData.clone();
//			migrations.add(new ETL_DM_KYCS(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_KYC_SENIOR_OFFICER_INFO(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_ALERT_ACCOUNT_BALANCE(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_KYC_BENEFICIARIES_INFO(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_KYC_PARTY(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_KYC_PARTY_ADDRESS(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_ALERT_ACCOUNT(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_ALERT_ADDRESS(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_ALERT_LOAN(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_ALERT_LOAN_MASTER(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_ALERT_PARTY(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_ALERT_LOAN_DETAIL(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_ALERT_PARTY_ADDRESS(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_ALERT_PARTY_EMAIL(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_ALERT_PARTY_GROUP(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_ALERT_PARTY_GROUP_MEMBER(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_ALERT_PARTY_ID(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_ALERT_PARTY_PARTY_REL(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_ALERT_PARTY_PHONE(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_ALERT_SERVICE(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_ALERT_TRANSACTION(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_ALERT_TRANSFER(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_ALERTS(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_CASE_SAR_REPORT(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_CASES(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_KYC_ACCOUNT(logData2));
			
			logData2 = logData.clone();
			migrations.add(new ETL_DM_PARTY_GROUP(logData2));
			
			logData2 = logData.clone();
			migrations.add(new ETL_DM_PARTY_GROUP_MEMBER(logData2));

			logData2 = logData.clone();
			migrations.add(new ETL_DM_USR_BLACK_LIST_ENTRY(logData2));
			
			logData2 = logData.clone();
			migrations.add(new ETL_DM_RISK_RANK(logData2));

			stepStr = "DM多線程";
			ExecutorService executor = Executors.newFixedThreadPool(5);

			for (Migration migration : migrations) {
				executor.execute(migration);
			}

			executor.shutdown();

			while (!executor.isTerminated()) {

			}

			System.out.println("線程池已經關閉");

		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println("執行" + stepStr + "發生錯誤:" + ex.getMessage());
		}

//		System.out.println("#### DMfunctionCMT End " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
		System.out.println("#### DMfunctionDomain End " + new SimpleDateFormat("yyyyMMdd HH:mm:ss").format(new Date()));
	}
	
	// 執行DataMigration後記錄檔
	public static void callDMfuncionMerge(ETL_Bean_DM_LogData logData) {
		try {
		
			// clone 新class指標用
			ETL_Bean_DM_LogData logData2 = logData.clone();
			new ETL_DM_ACCTMAPPING_MRG(logData2).migration_File();
			
			logData2 = logData.clone();
			new ETL_DM_TR_MAPPING_MRG(logData2).migration_File();
			
		} catch (Exception ex) {
			System.out.println("執行ETL_DM_ACCTMAPPING_MRG發生錯誤:" + ex.getMessage());
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		
		System.out.println("執行開始");
		
		ETL_Bean_DM_LogData newOne = new ETL_Bean_DM_LogData();
		newOne.setBatch_no("MIG00068");
//		newOne.setBatch_no("MIG00085");
		newOne.setCentral_no("600");
		Date record_date = new SimpleDateFormat("yyyyMMdd").parse("20180713");
//		Date record_date = new SimpleDateFormat("yyyyMMdd").parse("20180608");
		newOne.setRecord_date(record_date);
		newOne.setUpload_no("001");
		
//		newOne.setEnd_datetime(new Timestamp(new Date().getTime()) );
//		newOne.setExe_result("3");
//		newOne.setExe_result_description("4");
//		newOne.setFailed_cnt(5);
//		newOne.setFile_name("6");
//		newOne.setFile_type("7");
//		newOne.setSrc_file("8");
//		newOne.setStart_datetime(new Timestamp(new Date().getTime()));
//		newOne.setStep_type("9");
//		newOne.setSuccess_cnt(10);
//		newOne.setTotal_cnt(11);

//		DMfunction.callDMfunctionETL(newOne);
//		DMfunction.callDMfunctionCMT(newOne);
		
		System.out.println("執行結束");
	}
	
}
