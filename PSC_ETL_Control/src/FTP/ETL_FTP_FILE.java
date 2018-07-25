package FTP;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import Tool.ETL_Tool_DM_ParseFileName;
import Tool.ETL_Tool_StringX;

public class ETL_FTP_FILE extends File {
	private String savePath;

	public ETL_FTP_FILE(String savePath) throws Exception {
		super(savePath);

		if (!this.exists()) {
			throw new Exception(savePath + " 此路徑不存在! 請確認");
		} else if (!this.isDirectory()) {
			throw new Exception(savePath + " 並非資料夾路徑! 請確認");
		} else if (!this.canRead()) {
			throw new Exception(savePath + " 此路徑無讀取權限! 請確認");
		}

		this.savePath = savePath;
	}

	public ETL_FTP_FILE(String pathname, String a, Date date) throws Exception {
		super(pathname);
		if (!this.exists()) {
			throw new Exception(pathname + " 此路徑不存在! 請確認");
		} else if (!this.isDirectory()) {
			throw new Exception(pathname + " 並非資料夾路徑! 請確認");
		} else if (!this.canRead()) {
			throw new Exception(pathname + " 此路徑無讀取權限! 請確認");
		}
	}

	public List<File> getTRTargetFileList(String fileTypeName, Date targetDate) {
		String[] fileNameArray = this.list();
		ETL_Tool_DM_ParseFileName pfn;
		List<File> tartgetFileList = new ArrayList<File>();
		String recordDateStr;
		String targetDateStr;

		for (int i = 0; i < fileNameArray.length; i++) {

			try {
				pfn = new ETL_Tool_DM_ParseFileName(fileNameArray[i]);
			} catch (Exception ex) {
				// 檔名解析中出現錯誤時, 印出錯誤 且 跳過此圈後續, 繼續迴圈
				System.out.println(ex.getMessage());
				continue;
			}
			recordDateStr = ETL_Tool_StringX.toUtilDateStr(pfn.getRecord_date(), "yyyy-MM-dd");
			targetDateStr = ETL_Tool_StringX.toUtilDateStr(targetDate, "yyyy-MM-dd");
			
			System.out.println("recordDateStr:" + recordDateStr);
			System.out.println("targetDateStr:" + targetDateStr);

			// 檔名分析後 符合檔名進入list
			if (pfn.getFile_name().equalsIgnoreCase(fileTypeName) && recordDateStr.equals(targetDateStr)) {

				System.out.println(savePath + "/" + fileNameArray[i]);

				File tempFile = new File(savePath + "/" + fileNameArray[i]);
				tartgetFileList.add(tempFile);
			}
		}

		return tartgetFileList;
	}

	public void getTestInformation(boolean isTest) {
		String[] fileNameArray = this.list();

		System.out.println("ETF_Tool_FileReader 所有檔名");
		for (int i = 0; i < fileNameArray.length; i++) {
			System.out.println(fileNameArray[i]);
		}
	}

	private static void testConstruct() {
		try {
			ETL_FTP_FILE obj = new ETL_FTP_FILE(
					"D:\\company\\pershing\\agribank\\FTP_Data\\018\\UPLOAD\\test\\888");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		testConstruct();
	}

}
