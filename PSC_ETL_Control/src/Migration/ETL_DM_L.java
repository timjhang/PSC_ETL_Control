package Migration;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import Bean.ETL_Bean_LogData;

public class ETL_DM_L {

	// 觸發DB2載入Procedure, 資料載入ACCTMAPPING_LOAD
	public static void trans_to_ACCTMAPPING_LOAD(ETL_Bean_LogData logData, String fedServer, String runTable) {

		System.out.println("#######Load - ETL_L_ACCTMAPPING - Start");

		ETL_DM_MIGRATION_DAO.trans_to_ACCTMAPPING_LOAD( logData,  fedServer,  runTable);

		System.out.println("#######Load - ETL_L_ACCTMAPPING - End");

	}

	// 觸發DB2載入Procedure, 資料載入BRANCHMAPPING_LOAD
	public static void trans_to_BRANCHMAPPING_LOAD(ETL_Bean_LogData logData, String fedServer, String runTable) {

		System.out.println("#######Load - ETL_L_BRANCHMAPPING - Start");

		ETL_DM_MIGRATION_DAO. trans_to_BRANCHMAPPING_LOAD( logData,  fedServer,  runTable);

		System.out.println("#######Load - ETL_L_BRANCHMAPPING - End");

	}
	
	// 觸發DB2載入Procedure, 資料載入IDMAPPING_LOAD
	public static void trans_to_IDMAPPING_LOAD(ETL_Bean_LogData logData, String fedServer, String runTable) {

		System.out.println("#######Load - ETL_L_IDMAPPING - Start");

		ETL_DM_MIGRATION_DAO.trans_to_IDMAPPING_LOAD( logData,  fedServer,  runTable);

		System.out.println("#######Load - ETL_L_IDMAPPING - End");

	}
	
	public static void main(String args[]) throws ParseException {
		String batch_No = "DMTEST9";
		String exc_central_no = "600";
		String record_DateStr = "20180510";
		String before_record_dateStr = "20180509";
		String upload_No = "002";
		String server_No = "ETL_S1";
		String tableType = "temp";

		ETL_Bean_LogData logData = new ETL_Bean_LogData();
		logData.setBATCH_NO(batch_No);
		logData.setCENTRAL_NO(exc_central_no);
		logData.setFILE_TYPE("");
		Date exc_record_date = new SimpleDateFormat("yyyyMMdd").parse(record_DateStr);
		logData.setRECORD_DATE(exc_record_date);
		logData.setUPLOAD_NO(upload_No);
		Date before_record_date = new SimpleDateFormat("yyyyMMdd").parse(before_record_dateStr);
		logData.setBEFORE_ETL_PROCESS_DATE(before_record_date);

		String fedServer = "";
		// 設定fedName
		if ("ETL_S1".equals(server_No)) {
			fedServer = "ETLDB001";
		} else if ("ETL_S2".equals(server_No)) {
			fedServer = "ETLDB002";
		}

		// 執行用Table (正常 temp, 重跑rerun)
		String runTable = tableType;
		// String runTable = "rerun"; // test temp 2018.04.23 TimJhang

		System.out.println("batch_No:" + batch_No);
		System.out.println("exc_central_no:" + exc_central_no);
		System.out.println("exc_record_date:" + exc_record_date);
		System.out.println("upload_No:" + upload_No);
		System.out.println("server_No:" + server_No);
		System.out.println("runTable:" + runTable);
		logData.setPROGRAM_NO("ETL_L_ACCTMAPPING");
		ETL_DM_L.trans_to_ACCTMAPPING_LOAD(logData, fedServer, runTable);
		logData.setPROGRAM_NO("ETL_L_BRANCHMAPPING");
		ETL_DM_L.trans_to_BRANCHMAPPING_LOAD(logData, fedServer, runTable);

	}
	
}
