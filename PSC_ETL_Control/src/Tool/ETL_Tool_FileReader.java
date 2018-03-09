package Tool;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ETL_Tool_FileReader {
	
	public static boolean forTest = false;
	
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
					// 檔名解析中出現錯誤時, 印出錯誤  且  跳過此圈後續, 繼續迴圈
//					System.out.println(ex.getMessage());
					continue;
				}
					
				if (forTest) { // test
					System.out.println("ETF_Tool_FileReader 解析得" + pfn.getFileName() + " => " + pfn.getFile_Name());
				}
				
				// 檔名分析後  符合檔名進入list
				if (pfn.getFile_Name().equals(fileTypeName)) { 
					
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
					// 檔名解析中出現錯誤時, 印出錯誤  且  跳過此圈後續, 繼續迴圈
					System.out.println(ex.getMessage());
					continue;
				}
					
				if (forTest) { // test
					System.out.println("ETF_Tool_FileReader 解析得" + pfn.getFileName() + " => " + pfn.getFile_Name());
				}
				
				// 檔名分析後  符合檔名進入list
				if (pfn.getFile_Name().equals(fileTypeName)) { 
					
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

}
