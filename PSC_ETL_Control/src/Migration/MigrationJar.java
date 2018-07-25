package Migration;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

import Control.ETL_C_Master;
import Control.ETL_C_Migration;
import DB.ETL_P_Log;
import Tool.ETL_Tool_FormatCheck;
import Tool.ETL_Tool_StringX;


public class MigrationJar {
	
	private static String[] CENTRAL_NO_ARR = {"600","952"};

	public static void main(String[] args) throws Exception {
		String record_date_str = "", central_no = "";

		System.out.print("請輸入日期(格式yyyyMMdd): ");
		Scanner scanner = new Scanner(System.in);
		record_date_str = scanner.next();

		System.out.print("請輸入單位: ");
		scanner = new Scanner(System.in);
		central_no = scanner.next();

		
		if(!checkDate(record_date_str,central_no)) {
			return;
		}else {
			System.out.println("Migration 檢核通過");
		}
		
		Date record_date = ETL_Tool_StringX.toUtilDate(record_date_str);	
		
		//第一階段檢查通過 	註冊TABLE為 Ready	
		if(ETL_DM_MIGRATION_DAO.updateNewGenerationMigrationStatus( record_date,  central_no, "READY", "")) {
			System.out.println("Migration 註冊成功待執行");
		};
		
	}
	
	private static boolean isStartStatus(Date record_date, String central_no) {
		int count = ETL_DM_MIGRATION_DAO.get_Migration_Status_Count(record_date, central_no, "START");

		if (count != 0) {
			return true;
		} else {
			return false;
		}
	}
	
	private static boolean isWaitStatus(Date record_date, String central_no) {
		int count = ETL_DM_MIGRATION_DAO.get_Migration_Status_Count(record_date, central_no, "WAIT");

		if (count != 0) {
			return true;
		} else {
			return false;
		}
	}
	
	private static boolean checkDate(String record_date_str, String central_no) {

		try {
			if (!ETL_Tool_FormatCheck.checkDate(record_date_str)) {
				System.out.println("日期格式錯誤");
				return false;
			}

			boolean isSuccess = false;
			for (String str : CENTRAL_NO_ARR) {
				if (str.equals(central_no)) {
					isSuccess = true;
				}
			}

			if (!isSuccess) {
				System.out.println("單位輸入錯誤");
				return false;
			}

			Date record_date = ETL_Tool_StringX.toUtilDate(record_date_str);

			if (!isWaitStatus(record_date, central_no)) {
				System.out.println("狀態錯誤:對照檔、分支機構檔未完成");
				return false;
			}

			if (!ETL_C_Migration.checkHasMigrationMasterTxt(central_no)) {
				System.out.println("狀態錯誤:合併單位異動檔資料未上傳");
				return false;
			}

			if (!ETL_C_Master.checkHasMasterTxt(central_no)) {
				System.out.println("狀態錯誤:日常異動檔資料未上傳");
				return false;
			}
			
			
			//日常異動檔Master txt內容 檢查
			if(!checkMasterTxtContent( central_no, record_date_str)) {
				return false;
			}
			
			
			//合併單位異動檔Master txt內容 檢查
			if(!checkMigrationMasterTxtContent( central_no, record_date_str)) {
				return false;
			}
			

			if (isStartStatus(record_date, central_no)) {
				System.out.println("狀態錯誤:該單位正在執行");
				return false;
			}
			
		} catch (Exception e) {
			ETL_P_Log.write_Runtime_Log("DM", e.getMessage());
			System.out.println("狀態錯誤:執行失敗");
			return false;
		}

		return true;
	}
	
	private static boolean checkMasterTxtContent(String central_No,String record_date_str) throws Exception {
		
		List<String> zipFiles = new ArrayList<String>();
		
		// 取得 List<資料日期|上傳批號 |zip檔名>
		zipFiles = ETL_C_Master.parseMasterTxtContent(central_No);
		
		if (zipFiles == null) {
			System.out.println("狀態錯誤:" + central_No + "MasterTxt 資料錯誤");
			return false;
		}
		
		String[] dataInfo = zipFiles.get(0).split("\\|");
		
		if(!record_date_str.equals(dataInfo[0])) {
			System.out.println("狀態錯誤:"+ central_No+"MasterTxt 日期錯誤");
			return false;
		}

		return true;
	}
		

	private static boolean checkMigrationMasterTxtContent(String central_No, String record_date_str) throws Exception {

		List<String> zipFiles = new ArrayList<String>();

		// 取得 List<資料日期|上傳批號 |zip檔名>
		zipFiles = ETL_C_Migration.parseMigrationMasterTxtContent(central_No);
		
		if (zipFiles == null) {
			System.out.println("狀態錯誤:" + central_No + "MigrationMaster 資料錯誤");
			return false;
		}
		
		
		String[] dataInfo = zipFiles.get(0).split("\\|");

		// 確認是否為正確資料日期
		if (!dataInfo[0].equals(record_date_str)) {
			System.out.println("狀態錯誤:" +central_No + " MigrationMaster 日期錯誤:" + dataInfo[0]); // for test
			return false;
		}

		return true;
	}

}
