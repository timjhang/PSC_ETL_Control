package Tool;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import FTP.ETL_FTP_FILE;
import FTP.ETL_SFTP;

public class ETL_Tool_FileReader {

	public static boolean forTest = true;

	// 取得目標檔名資料List
	public static List<File> getTargetFileList(String filePath, String fileTypeName) throws Exception {

		List<File> tartgetFileList = new ArrayList<File>();

		try {

			// 讀取路徑, 先檢查相關權限是否ok
			File file = new File(filePath);
			if (!file.exists()) {
				throw new Exception(filePath + " 此路徑不存在! 請確認");
			} else if (!file.isDirectory()) {
				throw new Exception(filePath + " 並非資料夾路徑! 請確認");
			} else if (!file.canRead()) {
				throw new Exception(filePath + " 此路徑無讀取權限! 請確認");
			}

			// 取得檔名list
			String[] fileNameArray = file.list();

			if (forTest) { // test
				System.out.println("ETF_Tool_FileReader 所有檔名");
				for (int i = 0; i < fileNameArray.length; i++) {
					System.out.println(fileNameArray[i]);
				}
			}

			if (forTest) { // test
				System.out.println("ETF_Tool_FileReader 取得目標檔名" + fileTypeName);
			}

			ETL_Tool_ParseFileName pfn;
			for (int i = 0; i < fileNameArray.length; i++) {

				try {
					pfn = new ETL_Tool_ParseFileName(fileNameArray[i]);
				} catch (Exception ex) {
					// 檔名解析中出現錯誤時, 印出錯誤 且 跳過此圈後續, 繼續迴圈
					// System.out.println(ex.getMessage());
					continue;
				}

				if (forTest) { // test
					System.out.println("ETF_Tool_FileReader 解析得" + pfn.getFileName() + " => " + pfn.getFile_Name());
				}

				// 檔名分析後 符合檔名進入list
				// TODO V6 START
				// if (pfn.getFile_Name().equals(fileTypeName)) {
				if (pfn.getFile_Name().equalsIgnoreCase(fileTypeName)) {
					// TODO V6 END

					File tempFile = new File(filePath + "/" + fileNameArray[i]);
					tartgetFileList.add(tempFile);
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		// 回傳目標檔案File List
		return tartgetFileList;
	}

	// 取得目標檔名資料List, 針對無業務別檔案
	public static List<File> getTargetFileList_noFT(String filePath, String fileTypeName) throws Exception {

		List<File> tartgetFileList = new ArrayList<File>();

		try {

			// 讀取路徑, 先檢查相關權限是否ok
			File file = new File(filePath);
			if (!file.exists()) {
				throw new Exception(filePath + " 此路徑不存在! 請確認");
			} else if (!file.isDirectory()) {
				throw new Exception(filePath + " 並非資料夾路徑! 請確認");
			} else if (!file.canRead()) {
				throw new Exception(filePath + " 此路徑無讀取權限! 請確認");
			}

			// 取得檔名list
			String[] fileNameArray = file.list();

			if (forTest) { // test
				System.out.println("ETF_Tool_FileReader 所有檔名");
				for (int i = 0; i < fileNameArray.length; i++) {
					System.out.println(fileNameArray[i]);
				}
			}

			if (forTest) { // test
				System.out.println("ETF_Tool_FileReader 取得目標檔名" + fileTypeName);
			}

			ETL_Tool_ParseFileName pfn;
			for (int i = 0; i < fileNameArray.length; i++) {

				try {
					pfn = new ETL_Tool_ParseFileName(fileNameArray[i], true);
				} catch (Exception ex) {
					// 檔名解析中出現錯誤時, 印出錯誤 且 跳過此圈後續, 繼續迴圈
					System.out.println(ex.getMessage());
					continue;
				}

				if (forTest) { // test
					System.out.println("ETF_Tool_FileReader 解析得" + pfn.getFileName() + " => " + pfn.getFile_Name());
				}

				// 檔名分析後 符合檔名進入list
				// TODO V6 START
				// if (pfn.getFile_Name().equals(fileTypeName)) {
				if (pfn.getFile_Name().equalsIgnoreCase(fileTypeName)) {
					// TODO V6 END

					File tempFile = new File(filePath + "/" + fileNameArray[i]);
					tartgetFileList.add(tempFile);
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		// 回傳目標檔案File List
		return tartgetFileList;
	}


	// 取得目標檔名資料List
	public static List<File> getTRTargetFileList(String hostName, String port, String username, String password,
			String directory, String savePath, String fileTypeName,Date targetDate) throws Exception {
		
		String recordDateStr = "";
		String targetDateStr = ""; 
		
		// 取得檔名list
		List<String> fileNameList = ETL_SFTP.listFiles(hostName, Integer.valueOf(port), username, password, directory);

		List<String> fileNameListAllow = new ArrayList<String>();

		ETL_Tool_DM_ParseFileName pfn;
		for (int i = 0; i < fileNameList.size(); i++) {

			try {
				pfn = new ETL_Tool_DM_ParseFileName(fileNameList.get(i));
			} catch (Exception ex) {
				continue;
			}
			
			recordDateStr = ETL_Tool_StringX.toUtilDateStr(pfn.getRecord_date(), "yyyy-MM-dd");
			targetDateStr = ETL_Tool_StringX.toUtilDateStr(targetDate, "yyyy-MM-dd");
			
			System.out.println("pfn.getFile_name:" + pfn.getFile_name());
			System.out.println("fileTypeName:" + fileTypeName);
			System.out.println("recordDateStr:" + recordDateStr);
			System.out.println("targetDateStr:" + targetDateStr);
			
			
			// 檔名分析後 符合檔名進入list
			if (pfn.getFile_name().equalsIgnoreCase(fileTypeName)&&(recordDateStr.equals(targetDateStr))) {
				fileNameListAllow.add(pfn.getFileName());
			}
		}

		System.out.println("fileNameListAllow.size:" + fileNameListAllow.size());

		// 下載檔案至本地
		for (int i = 0; i < fileNameListAllow.size(); i++) {
			ETL_SFTP.download(hostName, port, username, password, savePath + "\\" + fileNameListAllow.get(i),
					directory + fileNameListAllow.get(i));
		}

		// 從SFTP刪除檔案
		// for (int i = 0; i < fileNameListAllow.size(); i++) {
		// ETL_SFTP.delete(hostName, port, username, password, "/" +
		// fileNameListAllow.get(i));
		// }

		List<File> tartgetFileList = new ArrayList<File>();

		try {

			ETL_FTP_FILE file = new ETL_FTP_FILE(savePath);
		
			file.getTestInformation(forTest);

			if (forTest) { // test
				System.out.println("ETF_Tool_FileReader 取得目標檔名" + fileTypeName);
			}

			tartgetFileList = file.getTRTargetFileList(fileTypeName, targetDate);

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		// 回傳目標檔案File List
		return tartgetFileList;
	}
	
	
	// 取得目標檔名資料List
		public static List<String> getTRTargetFileList(String hostName, String port, String username, String password,
				String directory) throws Exception {
			
			String recordDateStr = "";
			String targetDateStr = ""; 
			
			// 取得檔名list
			List<String> fileNameList = ETL_SFTP.listFiles(hostName, Integer.valueOf(port), username, password, directory);

			List<String> fileNameListAllow = new ArrayList<String>();

			ETL_Tool_DM_ParseFileName pfn;
			for (int i = 0; i < fileNameList.size(); i++) {

				try {
					pfn = new ETL_Tool_DM_ParseFileName(fileNameList.get(i));
				} catch (Exception ex) {
					continue;
				}
				

				System.out.println("pfn.getFile_name:" + pfn.getFile_name());
		
				fileNameListAllow.add(pfn.getFileName());
			
			}

			//System.out.println("fileNameListAllow.size:" + fileNameListAllow.size());

		
			return fileNameListAllow;
		}
}
